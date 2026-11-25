"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.stream_source import StreamSourceService
from app.services.batch_slice import BatchSliceService
from app.services.change_capture import ChangeCaptureService
from app.services.schema_govern import SchemaGovernService
from app.services.watermark import WatermarkService
from app.services.late_data import LateDataService
from app.services.stream_lookup import StreamLookupService
from app.services.merge_plan import MergePlanService
from app.services.partition_scale import PartitionScaleService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.snapshot_handoff import SnapshotHandoffService
from app.services.data_reconcile import DataReconcileService
from app.services.backfill_job import BackfillJobService
from app.services.cold_archive import ColdArchiveService
from app.services.compaction import CompactionService
from app.services.lineage import LineageService
from app.services.changelog import ChangelogService
from app.services.retention import RetentionService
from app.services.table_format import TableFormatService

SERVICES: dict[str, BaseModuleService] = {
    "stream_source": StreamSourceService(),
    "batch_slice": BatchSliceService(),
    "change_capture": ChangeCaptureService(),
    "schema_govern": SchemaGovernService(),
    "watermark": WatermarkService(),
    "late_data": LateDataService(),
    "stream_lookup": StreamLookupService(),
    "merge_plan": MergePlanService(),
    "partition_scale": PartitionScaleService(),
    "snapshot_mgr": SnapshotMgrService(),
    "snapshot_handoff": SnapshotHandoffService(),
    "data_reconcile": DataReconcileService(),
    "backfill_job": BackfillJobService(),
    "cold_archive": ColdArchiveService(),
    "compaction": CompactionService(),
    "lineage": LineageService(),
    "changelog": ChangelogService(),
    "retention": RetentionService(),
    "table_format": TableFormatService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
