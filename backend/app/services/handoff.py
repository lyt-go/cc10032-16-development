"""Compatibility module for handoff domain service naming."""
from app.handoff import SnapshotHandoffRegistry
from app.services.snapshot_handoff import HandoffService, SnapshotHandoffService, service

Registry = SnapshotHandoffRegistry

__all__ = ["SnapshotHandoffRegistry", "SnapshotHandoffService", "HandoffService", "Registry", "service"]
