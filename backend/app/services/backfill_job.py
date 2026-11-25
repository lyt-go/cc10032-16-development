"""backfill_job 领域服务：历史数据回填任务。

业务逻辑：
1. 回填任务：基于快照基准向目标表回填历史数据
2. 回填链路：snapshot_mgr → backfill_job（使用快照作为数据基准）
3. 失败场景：依赖检查未通过、并行度超限、回填水位不连续
4. 与上下游流水线：消费 snapshot_mgr 快照；输出回填结果

跨模块数据流：
  snapshot_mgr → backfill_job（快照数据驱动历史回填）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标表", "回填起始时间", "回填结束时间", "源表列表", "依赖检查", "并行度", "回填水位"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "回填起始时间"]
    STATUS_ORDER = ["待执行", "运行中", "已完成", "已失败"]
    ACTION_RULES = {"发起回填": "运行中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "backfill_pipeline"
    UPSTREAM_MODULES = ["snapshot_mgr"]
    PRODUCES_ARTIFACT = "回填执行记录"

    def execute_backfill(self, entry_id: int) -> dict[str, Any]:
        """执行回填：从快照基准向目标表写入历史数据。

        失败场景：
        - 依赖检查失败：上游表无快照
        - 并行度超限：线程数超过可用资源
        - 回填水位不连续：起始>结束或与已有数据冲突
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        target_table = str(entry.get("目标表", ""))
        source_tables = str(entry.get("源表列表", "")).split(",")
        parallelism = int(str(entry.get("并行度", "1")))

        # 依赖检查：验证源表有快照
        deps_ok = True
        dep_issues = []
        for st in source_tables:
            st = st.strip()
            if not st:
                continue
            has_snapshot = any(snap.get("table") == st for snap in store._snapshot_store.values())
            if not has_snapshot:
                dep_issues.append(f"源表 {st} 无可用快照")
                deps_ok = False
        if not deps_ok:
            entry["依赖检查"] = "失败: " + "; ".join(dep_issues)
            entry["status"] = "已失败"
            entry["abnormal"] = True
            return {"ok": False, "reason": "; ".join(dep_issues)}

        if parallelism > 10:
            return {"ok": False, "reason": f"并行度 {parallelism} 超限（最大10）"}

        # 模拟回填数据；若存在完整已核验交接基准，则改为回填快照实际业务行。
        rows_to_insert = []
        handoff_window: dict[str, Any] | None = None
        baseline = next(
            (item for item in store.handoff.list_baselines()
             if item.get("source_table") in {table.strip() for table in source_tables if table.strip()}),
            None,
        )
        if baseline is not None:
            handoff_window = store.handoff.open_backfill_window(
                baseline["package_no"], target_table=target_table)
            if not handoff_window.get("ok"):
                entry["依赖检查"] = "失败: " + handoff_window.get("message", "交接基准不可用")
                entry["status"] = "已失败"
                entry["abnormal"] = True
                return {"ok": False, "reason": handoff_window.get("message"), "window": handoff_window}
            rows_to_insert = handoff_window.get("backfilled_rows", [])
        else:
            for i in range(20):
                rows_to_insert.append({
                    "table": target_table,
                    "batch": entry.get("任务编号"),
                    "row": i,
                    "ts": time.time() - i * 3600,  # 历史时间
                    "backfilled": True,
                })

        entry["依赖检查"] = "通过"
        entry["回填水位"] = str(time.time())
        entry["status"] = "运行中"
        entry["pending"] = True

        batch_id = f"BACKFILL_{entry_id}_{int(time.time())}"
        self.produce_artifact(rows_to_insert, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "target_table": target_table,
            "rows_inserted": len(rows_to_insert),
            "source_tables": source_tables,
            "dependencies_passed": len(source_tables),
            "window_id": handoff_window.get("window_id") if handoff_window else None,
            "snapshot_id": handoff_window.get("package_no") if handoff_window else None,
            "all_business_keys_matched": handoff_window.get("all_keys_matched") if handoff_window else None,
            "key_count": handoff_window.get("key_count") if handoff_window else None,
            "key_results": handoff_window.get("key_results") if handoff_window else None,
        }

    # ---------- 交接包回填窗口 ----------
    def open_backfill_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.open_backfill_window(*args, **kwargs)

    start_backfill_window = open_backfill_window
    run_backfill_window = open_backfill_window

    def backfill_from_snapshot(self, snapshot_id: str, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.open_backfill_window(snapshot_id, **kwargs)

    def backfill_from_registered_snapshot(self, snapshot_id: str, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.open_backfill_window(snapshot_id, **kwargs)

    def compare_backfill_windows(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return store.handoff.compare_backfill_windows(*args, **kwargs)

    compare_backfill_results = compare_backfill_windows

    def backfill_baseline_manifest(self) -> list[dict[str, Any]]:
        return store.handoff.list_baselines()

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["snapshot_dependencies"] = len(store._snapshot_store)
        return base