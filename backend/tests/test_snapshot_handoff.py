"""快照交接包验收测试：分段续传、凭证核对、回填比对、并发冲突和另行登记。"""
from __future__ import annotations

import pytest

from app.handoff import SnapshotHandoffRegistry
from app.store import store


def _rows(batch: str, count: int = 4) -> list[dict]:
    return [
        {"id": 1000 + index, "order_no": f"{batch}-{index}", "amount": (index + 1) * 10, "batch": batch}
        for index in range(count)
    ]


class TestSnapshotHandoffAcceptance:
    def test_two_segments_disconnect_resume_and_compare_every_business_key(self):
        registry = SnapshotHandoffRegistry()
        rows = _rows("甲", 4)
        package = registry.export_package(
            "dwd_orders_handoff", rows,
            package_no="ACCEPT-RESUME", snapshot_version="v20261125", segment_count=2,
            maintainer="maintainer-a",
        )

        verification = registry.verify_package(package)
        assert verification["ok"] is True
        assert verification["source_table"] == "dwd_orders_handoff"
        assert verification["snapshot_version"] == "v20261125"
        assert [item["row_count"] for item in verification["segment_results"]] == [2, 2]
        assert verification["content_credential"] == package["content_credential"]

        started = registry.start_import(package)
        upload_id = started["upload_id"]
        first = registry.upload_segment(upload_id, 1, package["segments"][0])
        assert first["ok"] is True
        assert first["verified_segment_nos"] == [1]
        assert first["missing_segment_nos"] == [2]

        interrupted = registry.interrupt_import(upload_id)
        assert interrupted["interrupted"] is True
        assert interrupted["resumable"] is True

        # “重连”后只凭原 upload_id 读取并恢复验证进度。
        progress = registry.get_import_progress(upload_id)
        assert progress["verified_segment_nos"] == [1]
        assert progress["next_segment_no"] == 2
        resumed = registry.recover_import(upload_id)
        assert resumed["resumed"] is True

        second = registry.upload_segment(upload_id, 2, package["segments"][1])
        completed = registry.complete_import(upload_id)
        assert second["ok"] is True
        assert completed["ok"] is True
        assert completed["registered"] is True
        assert completed["baseline"]["status"] == "complete_verified"

        window = registry.open_backfill_window("ACCEPT-RESUME", target_table="backfill_orders")
        assert window["ok"] is True
        assert window["row_count"] == 4
        assert window["key_count"] == 4
        assert window["all_business_keys_matched"] is True
        assert {item["key"] for item in window["key_results"]} == {
            str(row["id"]) for row in rows
        }
        for key_result in window["key_results"]:
            assert key_result["matched"] is True
            assert key_result["expected"] == key_result["actual"]

        comparison = registry.compare_backfill_windows(
            *registry.window_ids_by_no["ACCEPT-RESUME"][-2:]
        ) if len(registry.window_ids_by_no["ACCEPT-RESUME"]) >= 2 else None
        # 同一基准再开一个窗口，两次完整回填必须一致。
        second_window = registry.open_backfill_window("ACCEPT-RESUME")
        comparison = registry.compare_backfill_windows(window["window_id"], second_window["window_id"])
        assert comparison["results_equal"] is True
        assert comparison["key_comparison"]["all_matched"] is True

    def test_duplicate_import_confirms_same_version_and_content(self):
        registry = SnapshotHandoffRegistry()
        package = registry.export_package(
            "dwd_users_handoff", _rows("same", 2),
            package_no="ACCEPT-DUPLICATE", snapshot_version="v7", segment_count=1,
        )
        first = registry.import_package(package)
        second = registry.import_package(package)

        assert first["ok"] is True
        assert first["status"] == "registered"
        assert second["ok"] is True
        assert second["duplicate"] is True
        assert second["idempotent"] is True
        assert second["status"] == "duplicate"
        assert second["state"] == "already_registered"
        baseline_versions = [
            item for item in registry.baselines if item["package_no"] == "ACCEPT-DUPLICATE"
        ]
        assert len(baseline_versions) == 1

    def test_same_version_changed_content_is_quarantined(self):
        registry = SnapshotHandoffRegistry()
        original = registry.export_package(
            "dwd_trades_handoff", _rows("original", 3),
            package_no="ACCEPT-MUTATED", snapshot_version="v9", segment_count=1,
        )
        changed_rows = _rows("changed", 3)
        changed_rows[1]["amount"] += 999
        changed = registry.export_package(
            "dwd_trades_handoff", changed_rows,
            package_no="ACCEPT-MUTATED", snapshot_version="v9", segment_count=1,
        )

        assert registry.import_package(original)["ok"] is True
        conflict = registry.import_package(changed)
        assert conflict["ok"] is False
        assert conflict["quarantined"] is True
        assert conflict["status"] == "quarantined"
        assert conflict["retained_candidate"]["content_credential"] == original["content_credential"]
        assert "另行登记" in conflict["next_action_for_loser"]
        assert len(registry.quarantine) == 1

        window = registry.open_backfill_window("ACCEPT-MUTATED")
        assert window["ok"] is True
        assert window["content_credential"] == original["content_credential"]
        assert window["rows"][1]["amount"] == 20

    def test_first_claim_policy_blocks_until_retained_a_completes_then_b_registers_separately(self):
        registry = SnapshotHandoffRegistry(conflict_policy="first_verified_segment_wins")
        rows_a = _rows("package-a", 4)
        rows_b = _rows("package-b", 4)
        package_a = registry.export_package(
            "dwd_concurrent", rows_a,
            package_no="ACCEPT-CONFLICT", snapshot_version="v3", segment_count=2,
            maintainer="maintainer-a", batch_id="batch-a",
        )
        package_b = registry.export_package(
            "dwd_concurrent", rows_b,
            package_no="ACCEPT-CONFLICT", snapshot_version="v3", segment_count=2,
            maintainer="maintainer-b", batch_id="batch-b",
        )

        upload_a = registry.start_import(package_a)
        segment_a1 = registry.upload_segment(upload_a["upload_id"], 1, package_a["segments"][0])
        assert segment_a1["ok"] is True

        conflict_b = registry.import_package(package_b)
        assert conflict_b["ok"] is False
        assert conflict_b["status"] == "registration_conflict"
        assert conflict_b["actual_retained_candidate"]["candidate_id"] == upload_a["candidate_id"]
        assert conflict_b["loser_candidate"]["maintainer"] == "maintainer-b"
        assert conflict_b["can_register_separately"] is True
        assert "新编号" in conflict_b["loser_next_step"] or "另行登记" in conflict_b["loser_next_step"]

        first_window = registry.open_backfill_window("ACCEPT-CONFLICT")
        assert first_window["ok"] is False
        assert first_window["backfilled"] is False
        assert first_window["status"] == "blocked_incomplete"
        assert first_window["missing_segment_nos"] == [2]
        assert "第 2 段" in first_window["message"]

        segment_a2 = registry.upload_segment(upload_a["upload_id"], 2, package_a["segments"][1])
        assert segment_a2["ok"] is True
        completed_a = registry.complete_import(upload_a["upload_id"])
        assert completed_a["ok"] is True
        assert completed_a["retained_candidate"]["candidate_id"] == upload_a["candidate_id"]

        second_window = registry.open_backfill_window("ACCEPT-CONFLICT")
        assert second_window["ok"] is True
        assert second_window["row_count"] == 4
        assert second_window["all_business_keys_matched"] is True
        assert [row["batch"] for row in second_window["rows"]] == ["package-a"] * 4

        comparison = registry.compare_backfill_windows(
            first_window["window_id"], second_window["window_id"]
        )
        assert comparison["first_status"] == "blocked_incomplete"
        assert comparison["second_status"] == "completed"
        assert comparison["both_completed"] is False
        assert comparison["results_equal"] is False
        assert comparison["second_row_count"] == 4

        alternative = registry.register_alternative(
            conflict_b["candidate_id"], new_package_no="ACCEPT-CONFLICT-B"
        )
        assert alternative["ok"] is True
        assert alternative["alternative_registered"] is True
        window_b = registry.open_backfill_window("ACCEPT-CONFLICT-B")
        assert window_b["ok"] is True
        assert window_b["all_business_keys_matched"] is True
        assert [row["batch"] for row in window_b["rows"]] == ["package-b"] * 4

        a_vs_b = registry.compare_rows_by_business_key(
            second_window["rows"], window_b["rows"], "id"
        )
        assert a_vs_b["all_matched"] is False
        assert len(a_vs_b["changed_keys"]) == 4

    def test_default_policy_uses_complete_b_then_keeps_baseline_stable_after_a_finishes(self):
        registry = SnapshotHandoffRegistry()
        rows_a = _rows("package-a", 4)
        rows_b = _rows("package-b", 4)
        package_a = registry.export_package(
            "dwd_concurrent_default", rows_a,
            package_no="ACCEPT-CONFLICT-DEFAULT", snapshot_version="v3", segment_count=2,
            maintainer="maintainer-a",
        )
        package_b = registry.export_package(
            "dwd_concurrent_default", rows_b,
            package_no="ACCEPT-CONFLICT-DEFAULT", snapshot_version="v3", segment_count=2,
            maintainer="maintainer-b",
        )

        upload_a = registry.start_import(package_a)
        registry.upload_segment(upload_a["upload_id"], 1, package_a["segments"][0])

        registered_b = registry.import_package(package_b)
        assert registered_b["ok"] is True
        assert registered_b["conflict"] is True
        assert registered_b["status"] == "registered_with_concurrent_candidate"
        assert registered_b["candidate_id"] == registered_b["actual_retained_candidate"]["candidate_id"]
        assert registered_b["loser_candidate"]["candidate_id"] == upload_a["candidate_id"]
        assert registered_b["can_register_separately"] is True

        first_window = registry.open_backfill_window("ACCEPT-CONFLICT-DEFAULT")
        assert first_window["ok"] is True
        assert [row["batch"] for row in first_window["rows"]] == ["package-b"] * 4
        assert first_window["all_business_keys_matched"] is True
        assert first_window["blocked_claim"]["status"] == "blocked_incomplete"
        assert first_window["blocked_claim"]["missing_segment_nos"] == [2]
        assert "第 2 段" in first_window["pending_candidate_notice"]

        registry.upload_segment(upload_a["upload_id"], 2, package_a["segments"][1])
        completed_a = registry.complete_import(upload_a["upload_id"])
        assert completed_a["ok"] is False
        assert completed_a["quarantined"] is True
        assert completed_a["retained_candidate"]["content_credential"] == package_b["content_credential"]

        second_window = registry.open_backfill_window("ACCEPT-CONFLICT-DEFAULT")
        assert [row["batch"] for row in second_window["rows"]] == ["package-b"] * 4
        comparison = registry.compare_backfill_windows(
            first_window["window_id"], second_window["window_id"]
        )
        assert comparison["both_completed"] is True
        assert comparison["results_equal"] is True

        alternative = registry.register_alternative(
            upload_a["candidate_id"], new_package_no="ACCEPT-CONFLICT-DEFAULT-A"
        )
        assert alternative["ok"] is True
        window_a = registry.open_backfill_window("ACCEPT-CONFLICT-DEFAULT-A")
        assert [row["batch"] for row in window_a["rows"]] == ["package-a"] * 4

    def test_incomplete_only_candidate_reports_exact_missing_segment(self):
        registry = SnapshotHandoffRegistry()
        package = registry.export_package(
            "dwd_incomplete_only", _rows("partial", 4),
            package_no="ACCEPT-INCOMPLETE-ONLY", segment_count=2,
        )
        upload = registry.start_import(package)
        registry.upload_segment(upload["upload_id"], 1, package["segments"][0])

        window = registry.open_backfill_window("ACCEPT-INCOMPLETE-ONLY")
        assert window["ok"] is False
        assert window["status"] == "blocked_incomplete"
        assert window["missing_segment_nos"] == [2]
        assert "第 2 段" in window["message"]

    def test_global_store_syncs_baseline_to_snapshot_and_backfill_manifest(self):
        code = "ACCEPT-GLOBAL-SYNC"
        package = store.handoff.export_package(
            "dwd_global_handoff", _rows("global", 2),
            package_no=code, snapshot_version="v1", segment_count=1,
        )
        result = store.handoff.import_package(package)
        assert result["ok"] is True

        stored_snapshot = store.get_snapshot(code)
        assert stored_snapshot is not None
        assert stored_snapshot["handoff_verified"] is True
        assert stored_snapshot["row_count"] == 2
        assert len(stored_snapshot["rows"]) == 2

        manifest = store.handoff.list_baselines()
        assert any(item["package_no"] == code for item in manifest)
        snapshot_row = next(row for row in store.rows("snapshot_mgr") if row.get("快照编号") == code)
        assert snapshot_row["status"] == "已完成"
        assert snapshot_row["行数统计"] == "2"
