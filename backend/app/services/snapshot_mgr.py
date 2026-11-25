"""snapshot_mgr 领域服务：数据快照版本管理。

业务逻辑：
1. 快照创建：对源表生成版本化快照（存储路径+压缩格式+校验摘要）
2. 快照链路：snapshot_mgr → backfill_job（快照用于历史回填基准）
3. 失败场景：校验摘要不匹配、存储路径不可达、压缩格式错误
4. 与上下游流水线：为 backfill_job 提供回填基准快照

跨模块数据流：
  snapshot_mgr → backfill_job（快照作为回填数据基准）
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "数据快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "源表名称", "快照时间", "快照版本", "行数统计", "存储路径", "压缩格式", "校验摘要"]
    REQUIRED_FIELDS = ["快照编号", "源表名称", "快照时间"]
    STATUS_ORDER = ["创建中", "已完成", "已恢复", "已过期"]
    ACTION_RULES = {"创建快照": "已完成", "标记完成": "已恢复", "恢复快照": "已过期"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "snapshot_management"
    DOWNSTREAM_MODULES = ["backfill_job"]
    PRODUCES_ARTIFACT = "数据版本快照"

    def create_snapshot(self, entry_id: int, source_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """创建数据快照：对源表数据做校验摘要和压缩。

        失败场景：
        - 存储路径不可达
        - 压缩格式不支持
        - 校验摘要生成失败
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"快照 {entry_id} 不存在"}

        table_name = str(entry.get("源表名称", ""))
        snap_version = str(entry.get("快照版本", f"v{int(time.time())}"))
        compress_fmt = str(entry.get("压缩格式", "parquet"))

        if not table_name:
            return {"ok": False, "reason": "源表名称为空，无法创建快照"}

        if compress_fmt not in ("parquet", "orc", "avro"):
            return {"ok": False, "reason": f"不支持的压缩格式: {compress_fmt}"}

        # 计算校验摘要
        raw = "".join(str(r) for r in source_rows[:100])
        checksum = hashlib.md5(raw.encode()).hexdigest()[:16]

        storage_path = f"s3://data-lake/snapshots/{table_name}/{snap_version}/"
        row_count = len(source_rows)

        # 更新条目
        entry["快照版本"] = snap_version
        entry["行数统计"] = str(row_count)
        entry["存储路径"] = storage_path
        entry["校验摘要"] = checksum
        entry["快照时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "已完成"
        entry["pending"] = False

        # 保存完整行供同进程回填使用；data_sample 仅用于列表/旧接口预览。
        store.save_snapshot(entry["快照编号"], {
            "table": table_name,
            "version": snap_version,
            "path": storage_path,
            "checksum": checksum,
            "row_count": row_count,
            "rows": source_rows,
            "data_sample": source_rows[:3],
            "handoff_verified": False,
        })

        batch_id = f"SNAP_{entry_id}_{int(time.time())}"
        self.produce_artifact(source_rows[:50], batch_id)

        return {
            "ok": True,
            "snapshot_id": entry["快照编号"],
            "table": table_name,
            "version": snap_version,
            "rows": row_count,
            "checksum": checksum,
            "path": storage_path,
        }

    # ---------- 完整交接包导出/导入 ----------
    def export_snapshot(self, snapshot: Any = None, rows: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """兼容多种调用：直接传 rows/table，或传快照 entry_id/编号导出已存完整行。"""
        if rows is not None and "rows" not in kwargs:
            kwargs["rows"] = rows
        if snapshot is not None and not isinstance(snapshot, (list, dict)):
            text = str(snapshot)
            entry = None
            is_identifier = (
                isinstance(snapshot, int)
                or text.isdigit()
                or bool(kwargs.get("source_table") or kwargs.get("table") or kwargs.get("源表名称"))
            )
            if is_identifier and text.isdigit():
                entry = store.find(self.MODULE, int(text))
            if entry is not None:
                code = str(entry.get("快照编号", text))
            elif store.get_snapshot(text) is not None or text.startswith(("SNAP", "UPL", "BASE")):
                code = text
                is_identifier = True
            else:
                matching = next((row for row in store.rows(self.MODULE) if row.get("快照编号") == text), None)
                if matching is not None:
                    entry = matching
                    code = text
                    is_identifier = True
                else:
                    code = text

            if is_identifier:
                stored = store.get_snapshot(code)
                if stored and stored.get("rows") is not None and not kwargs.get("rows"):
                    kwargs.setdefault("rows", stored["rows"])
                    kwargs.setdefault("source_table", stored.get("table"))
                    kwargs.setdefault("snapshot_version", stored.get("version", "v1"))
                if entry:
                    kwargs.setdefault("package_no", entry.get("快照编号"))
                    kwargs.setdefault("source_table", entry.get("源表名称"))
                    kwargs.setdefault("snapshot_version", entry.get("快照版本", "v1"))
                else:
                    kwargs.setdefault("package_no", code)
                snapshot = None
            # 非编号字符串按 source_table 位置参数继续下传。
        if isinstance(snapshot, dict):
            return store.handoff.export_package(**{**snapshot, **kwargs})
        if isinstance(snapshot, list) and "rows" not in kwargs:
            kwargs["rows"] = snapshot
            snapshot = None
        if snapshot is not None:
            return store.handoff.export_package(snapshot, *args, **kwargs)
        return store.handoff.export_package(*args, **kwargs)

    def export_snapshot_package(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.export_snapshot(*args, **kwargs)

    def export_handoff_package(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.export_snapshot(*args, **kwargs)

    def export_snapshot_data(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.export_snapshot(*args, **kwargs)

    def verify_handoff_package(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.verify_package(*args, **kwargs)

    def start_handoff_import(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.start_import(*args, **kwargs)

    def upload_handoff_segment(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.upload_segment(*args, **kwargs)

    def complete_handoff_import(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.complete_import(*args, **kwargs)

    def import_handoff_package(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.import_package(*args, **kwargs)

    def import_snapshot_package(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.import_package(*args, **kwargs)

    def start_snapshot_import(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.start_import(*args, **kwargs)

    def upload_snapshot_segment(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.upload_segment(*args, **kwargs)

    def complete_snapshot_import(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.complete_import(*args, **kwargs)

    def get_handoff_progress(self, upload_id: str) -> dict[str, Any]:
        return store.handoff.get_import_progress(upload_id)

    def handoff_progress(self, upload_id: str) -> dict[str, Any]:
        return store.handoff.get_import_progress(upload_id)

    def backfill_baseline_manifest(self) -> list[dict[str, Any]]:
        return store.handoff.list_baselines()

    def emit_snapshot_for_backfill(self, table_name: str) -> dict[str, Any]:
        """向 backfill_job 暴露快照基准数据"""
        for snap_id, snap_data in store._snapshot_store.items():
            if snap_data.get("table") == table_name:
                return {"ok": True, "snapshot": snap_data, "downstream_consumer": "backfill_job"}
        return {"ok": False, "reason": f"表 {table_name} 无可用快照"}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stored_snapshots"] = len(store._snapshot_store)
        return base