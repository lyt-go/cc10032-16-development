"""兼容可能的命名：快照交接应用服务。"""
from app.services.snapshot_handoff import SnapshotHandoffService, service

HandoffService = SnapshotHandoffService
SnapshotHandoffAppService = SnapshotHandoffService
handoff_service = service

__all__ = ["SnapshotHandoffService", "HandoffService", "SnapshotHandoffAppService", "handoff_service"]
