"""快照交接应用服务：把核心登记逻辑挂到共享 store，并保留模块服务入口。"""
from __future__ import annotations

from typing import Any

from app.handoff import SnapshotHandoffRegistry
from app.services.base import BaseModuleService
from app.store import store


class SnapshotHandoffService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "快照交接包"

    def __init__(self, registry: SnapshotHandoffRegistry | None = None) -> None:
        # 默认使用进程内共享 registry，使 HTTP、领域服务和脚本看到同一份进度。
        self.registry = registry or store.handoff

    def __getattr__(self, name: str) -> Any:
        return getattr(self.registry, name)

    def export_from_snapshot_entry(self, entry_id: int, rows: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
        entry = store.find("snapshot_mgr", entry_id) or {}
        kwargs.setdefault("package_no", entry.get("快照编号"))
        kwargs.setdefault("source_table", entry.get("源表名称"))
        kwargs.setdefault("snapshot_version", entry.get("快照版本", "v1"))
        return self.registry.export_package(rows=rows, **kwargs)

    def baseline_manifest(self) -> list[dict[str, Any]]:
        return self.registry.list_baselines()

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base.update({
            "handoff_uploads": len(self.registry.uploads),
            "backfill_baselines": len(self.registry.baselines),
            "quarantined_packages": len(self.registry.quarantine),
            "conflicts": len(self.registry.conflicts),
            "backfill_windows": len(self.registry.windows),
        })
        return base


service = SnapshotHandoffService()
