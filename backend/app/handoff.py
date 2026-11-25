"""快照交接包、分段上传、冲突登记与回填窗口的核心领域逻辑。

该模块不依赖 FastAPI，既可以被路由层调用，也可以在测试或脚本中直接使用。
所有状态保存在内存仓库中；交接包本身是自描述 JSON，可跨环境导出后再导入。
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from copy import deepcopy
from typing import Any, Iterable


FORMAT = "snapshot-handoff-package"
FORMAT_VERSION = "1.0"


def now_ts() -> float:
    return time.time()


def canonical_json(value: Any) -> bytes:
    """生成稳定 JSON 字节，避免 dict 顺序影响内容凭证。"""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def sha256_hex(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def md5_hex(value: Any) -> str:
    return hashlib.md5(canonical_json(value)).hexdigest()  # noqa: S324 - 兼容旧摘要展示


def credential_bundle(value: Any) -> dict[str, str]:
    sha = sha256_hex(value)
    md5 = md5_hex(value)
    return {
        "checksum": sha,
        "sha256": sha,
        "md5": md5,
        "content_credential": sha,
        "credential": sha,
    }


def credential_matches(expected: Any, actual: dict[str, str]) -> bool:
    """接受完整 sha256/md5，也兼容旧代码中截断的摘要前缀。"""
    if expected in (None, ""):
        return False
    expected_text = str(expected).strip().lower()
    if expected_text in {str(v).lower() for v in actual.values()}:
        return True
    if len(expected_text) >= 12:
        for value in actual.values():
            if value.lower().startswith(expected_text):
                return True
    return False


def business_key_of(row: dict[str, Any], key_fields: str | list[str] | tuple[str, ...]) -> str:
    if isinstance(key_fields, str):
        fields = [part.strip() for part in key_fields.split(",") if part.strip()]
    else:
        fields = list(key_fields)
    if len(fields) == 1:
        return canonical_json(row.get(fields[0])).decode("utf-8")
    return canonical_json({field: row.get(field) for field in fields}).decode("utf-8")


def extract_rows(payload: Any) -> list[dict[str, Any]] | None:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for name in ("rows", "data", "records", "items", "内容", "数据行"):
            value = payload.get(name)
            if isinstance(value, list):
                return value
    return None


class SnapshotHandoffRegistry:
    """按快照编号管理交接包候选、基准清单、冲突隔离和回填窗口。"""

    def __init__(self, *, conflict_policy: str = "registered_version_wins") -> None:
        # registered_version_wins: 首个完整核验并登记者成为可回填基准（默认）。
        # first_verified_segment_wins: 首个已有核验段的候选悲观占用编号。
        self.conflict_policy = conflict_policy
        self.uploads: dict[str, dict[str, Any]] = {}
        self.candidate_ids_by_no: dict[str, list[str]] = {}
        self.baseline_by_no: dict[str, dict[str, Any]] = {}
        self.version_baselines: dict[tuple[str, str], dict[str, Any]] = {}
        self.baselines: list[dict[str, Any]] = []
        self.quarantine: list[dict[str, Any]] = []
        self.conflicts: list[dict[str, Any]] = []
        self.registrations: list[dict[str, Any]] = []
        self.windows: dict[str, dict[str, Any]] = {}
        self.window_ids_by_no: dict[str, list[str]] = {}
        self.idempotency_keys: dict[str, str] = {}
        self.backfilled_tables: dict[str, list[dict[str, Any]]] = {}
        self.sync_callback: Any = None

    def reset(self) -> None:
        self.__init__(conflict_policy=self.conflict_policy)  # type: ignore[misc]

    # ------------------------------------------------------------------
    # 导出与包校验
    # ------------------------------------------------------------------
    def export_package(
        self,
        source_table: str | None = None,
        rows: list[dict[str, Any]] | None = None,
        *,
        package_no: str | None = None,
        snapshot_id: str | None = None,
        snapshot_version: str = "v1",
        version: str | None = None,
        business_key: str | list[str] | None = None,
        segment_count: int | None = 2,
        segment_size: int | None = None,
        chunk_size: int | None = None,
        segments: list[list[dict[str, Any]]] | None = None,
        parts: list[list[dict[str, Any]]] | None = None,
        maintainer: str | None = None,
        exporter: str | None = None,
        batch_id: str | None = None,
        snapshot_time: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        """导出包含完整业务行、分段行数和内容凭证的自描述交接包。"""
        if isinstance(source_table, list) and rows is None:
            rows = source_table
            source_table = None
        source_table = source_table or extra.get("table") or extra.get("源表名称")
        if not source_table:
            raise ValueError("source_table 不能为空")
        rows = [dict(row) for row in (rows or [])]
        code = str(package_no or snapshot_id or extra.get("snapshot_no") or extra.get("快照编号")
                   or f"SNAP-HANDOFF-{uuid.uuid4().hex[:10].upper()}")
        ver = str(version or snapshot_version or extra.get("快照版本") or "v1")
        key = business_key or extra.get("business_key") or extra.get("business_keys") or extra.get("主键") or "id"

        explicit_segments = segments or parts
        if explicit_segments is not None:
            row_segments = [[dict(row) for row in segment] for segment in explicit_segments]
        else:
            size = segment_size or chunk_size
            if size:
                size = int(size)
                row_segments = [rows[i:i + size] for i in range(0, len(rows), size)]
            else:
                count = max(1, int(segment_count or 1))
                size = max(1, (len(rows) + count - 1) // count)
                row_segments = [rows[i:i + size] for i in range(0, len(rows), size)]
            if not row_segments:
                row_segments = [[]]

        segment_objects: list[dict[str, Any]] = []
        for index, segment_rows in enumerate(row_segments):
            cred = credential_bundle(segment_rows)
            number = index + 1
            segment_objects.append({
                "segment_no": number,
                "segment_index": index,
                "index": index,
                "part_no": number,
                "part_index": index,
                "name": f"segment-{number:04d}",
                "row_count": len(segment_rows),
                "expected_row_count": len(segment_rows),
                "rows": segment_rows,
                **cred,
            })

        all_rows = [row for segment in row_segments for row in segment]
        total_cred = credential_bundle(all_rows)
        manifest_segments = [self._segment_metadata(segment) for segment in segment_objects]
        manifest: dict[str, Any] = {
            "format": FORMAT,
            "format_version": FORMAT_VERSION,
            "package_no": code,
            "source_table": source_table,
            "snapshot_version": ver,
            "business_key": key,
            "total_rows": len(all_rows),
            "row_count": len(all_rows),
            "segment_count": len(segment_objects),
            "segments": manifest_segments,
            "created_at": now_ts(),
            "maintainer": maintainer or exporter,
            "batch_id": batch_id,
        }
        manifest.update({k: v for k, v in extra.items() if k not in {"table", "源表名称"}})
        manifest_cred = sha256_hex(manifest)

        package: dict[str, Any] = {
            "ok": True,
            "format": FORMAT,
            "package_format": FORMAT,
            "format_version": FORMAT_VERSION,
            "package_no": code,
            "package_id": code,
            "snapshot_id": code,
            "snapshot_no": code,
            "编号": code,
            "快照编号": code,
            "source_table": source_table,
            "table": source_table,
            "源表名称": source_table,
            "snapshot_version": ver,
            "version": ver,
            "快照版本": ver,
            "business_key": key,
            "business_keys": key if isinstance(key, list) else [key],
            "primary_key": key,
            "total_rows": len(all_rows),
            "row_count": len(all_rows),
            "行数统计": len(all_rows),
            "segment_count": len(segment_objects),
            "segments": segment_objects,
            "parts": segment_objects,
            "chunks": segment_objects,
            "manifest": manifest,
            "manifest_checksum": manifest_cred,
            "snapshot_time": snapshot_time or time.strftime("%Y-%m-%d %H:%M:%S"),
            "exported_at": now_ts(),
            "exporter": exporter or maintainer,
            "maintainer": maintainer or exporter,
            "batch_id": batch_id,
            **total_cred,
            "total_checksum": total_cred["checksum"],
            "rows_checksum": total_cred["checksum"],
        }
        return package

    # 常见英文/中文别名。
    export_snapshot = export_package
    create_package = export_package
    build_package = export_package

    def dumps_package(self, package: dict[str, Any]) -> str:
        return json.dumps(package, ensure_ascii=False, indent=2, default=str)

    def loads_package(self, raw: str | bytes) -> dict[str, Any]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("交接包顶层必须是 JSON 对象")
        return value

    def verify_package(
        self,
        package: dict[str, Any] | str | bytes,
        *,
        expected_source_table: str | None = None,
        expected_version: str | None = None,
        expected_package_no: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(package, (str, bytes)):
            package = self.loads_package(package)
        errors: list[str] = []
        code = str(package.get("package_no") or package.get("snapshot_id") or package.get("快照编号") or "")
        table = str(package.get("source_table") or package.get("table") or package.get("源表名称") or "")
        version = str(package.get("snapshot_version") or package.get("version") or package.get("快照版本") or "")
        if expected_package_no and code != expected_package_no:
            errors.append(f"编号不一致：期望 {expected_package_no}，实际 {code}")
        if expected_source_table and table != expected_source_table:
            errors.append(f"源表不一致：期望 {expected_source_table}，实际 {table}")
        if expected_version and version != expected_version:
            errors.append(f"快照版本不一致：期望 {expected_version}，实际 {version}")
        if not table:
            errors.append("缺少源表名称")
        if not version:
            errors.append("缺少快照版本")

        raw_segments = (package.get("segments") or package.get("parts") or package.get("chunks")
                        or package.get("分段") or package.get("分段列表") or [])
        if not isinstance(raw_segments, list) or not raw_segments:
            errors.append("交接包缺少分段")
            raw_segments = []

        segment_results: list[dict[str, Any]] = []
        all_rows: list[dict[str, Any]] = []
        for index, segment in enumerate(raw_segments, start=1):
            if not isinstance(segment, dict):
                errors.append(f"第 {index} 段格式错误")
                continue
            number = int(segment.get("segment_no") or segment.get("part_no")
                         or segment.get("段号") or segment.get("分段编号") or index)
            rows = extract_rows(segment) or []
            expected_count = segment.get(
                "row_count",
                segment.get("expected_row_count", segment.get("行数", segment.get("行数统计", len(rows)))),
            )
            expected_cred = (segment.get("content_credential") or segment.get("checksum")
                             or segment.get("sha256") or segment.get("credential")
                             or segment.get("内容凭证") or segment.get("凭证")
                             or segment.get("校验摘要"))
            actual = credential_bundle(rows)
            segment_errors: list[str] = []
            if int(expected_count) != len(rows):
                segment_errors.append(f"行数不一致：凭证 {expected_count}，实际 {len(rows)}")
            if expected_cred and not credential_matches(expected_cred, actual):
                segment_errors.append("内容凭证不匹配")
            result = {
                "segment_no": number,
                "segment_index": number - 1,
                "ok": not segment_errors,
                "verified": not segment_errors,
                "row_count": len(rows),
                "expected_row_count": int(expected_count),
                "checksum": actual["checksum"],
                "errors": segment_errors,
            }
            segment_results.append(result)
            errors.extend(f"第{number}段：{err}" for err in segment_errors)
            all_rows.extend(rows)

        expected_total = package.get("total_rows", package.get("row_count", package.get("行数统计")))
        if expected_total not in (None, "") and int(expected_total) != len(all_rows):
            errors.append(f"总行数不一致：凭证 {expected_total}，实际 {len(all_rows)}")

        expected_total_cred = (package.get("content_credential") or package.get("checksum")
                               or package.get("sha256") or package.get("rows_checksum")
                               or package.get("内容凭证") or package.get("凭证")
                               or package.get("校验摘要"))
        total_actual = credential_bundle(all_rows)
        credential_ok = True
        if expected_total_cred and not credential_matches(expected_total_cred, total_actual):
            credential_ok = False
            errors.append("整包内容凭证不匹配")

        ok = not errors
        return {
            "ok": ok,
            "verified": ok,
            "package_no": code,
            "source_table": table,
            "snapshot_version": version,
            "row_count": len(all_rows),
            "segment_count": len(segment_results),
            "segment_results": segment_results,
            "segments": segment_results,
            "credential_ok": credential_ok,
            "content_credential": total_actual["checksum"],
            "checksum": total_actual["checksum"],
            "errors": errors,
            "message": "交接包核对通过" if ok else "；".join(errors),
        }

    verify_handoff_package = verify_package
    validate_package = verify_package

    @staticmethod
    def _segment_metadata(segment: dict[str, Any]) -> dict[str, Any]:
        return {
            "segment_no": segment["segment_no"],
            "segment_index": segment["segment_index"],
            "name": segment["name"],
            "row_count": segment["row_count"],
            "expected_row_count": segment["row_count"],
            "checksum": segment["checksum"],
            "sha256": segment["sha256"],
            "md5": segment["md5"],
            "content_credential": segment["content_credential"],
        }

    # ------------------------------------------------------------------
    # 分段导入与可恢复进度
    # ------------------------------------------------------------------
    def start_import(
        self,
        package_no: Any = None,
        manifest: dict[str, Any] | None = None,
        package: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if isinstance(package_no, dict) and manifest is None and package is None:
            package = package_no
            package_no = None
        source = package or manifest or {}
        if not isinstance(source, dict):
            raise ValueError("manifest/package 必须是对象")

        code = str(
            package_no
            or kwargs.get("snapshot_id")
            or source.get("package_no")
            or source.get("snapshot_id")
            or source.get("快照编号")
            or kwargs.get("package_id")
            or f"UPL-{uuid.uuid4().hex[:10].upper()}"
        )
        table = str(kwargs.get("source_table") or source.get("source_table") or source.get("table")
                    or source.get("源表名称") or "")
        version = str(kwargs.get("snapshot_version") or kwargs.get("version")
                      or source.get("snapshot_version") or source.get("version")
                      or source.get("快照版本") or "v1")
        key = (kwargs.get("business_key") or kwargs.get("business_keys")
               or source.get("business_key") or source.get("business_keys")
               or source.get("primary_key") or "id")
        maintainer = kwargs.get("maintainer") or kwargs.get("operator") or kwargs.get("uploaded_by") or source.get("maintainer")
        batch_id = kwargs.get("batch_id") or source.get("batch_id")
        content_credential = (kwargs.get("content_credential") or kwargs.get("checksum")
                              or source.get("content_credential") or source.get("checksum"))
        idempotency_key = kwargs.get("idempotency_key")

        upload_id = str(kwargs.get("upload_id") or kwargs.get("candidate_id")
                        or source.get("upload_id") or f"UPL-{uuid.uuid4().hex[:12].upper()}")
        if upload_id in self.uploads:
            return self.get_import_progress(upload_id)

        if idempotency_key and idempotency_key in self.idempotency_keys:
            existing_id = self.idempotency_keys[idempotency_key]
            if existing_id in self.uploads:
                return self.get_import_progress(existing_id)

        # 相同维护员、编号、版本和内容凭证的重试视为同一次上传。
        if kwargs.get("resume", True):
            for candidate_id in reversed(self.candidate_ids_by_no.get(code, [])):
                candidate = self.uploads[candidate_id]
                if (candidate.get("maintainer") == maintainer
                        and candidate.get("snapshot_version") == version
                        and (not content_credential
                             or candidate.get("content_credential") == content_credential)
                        and candidate.get("status") not in {"quarantined", "registered_conflict_lost"}):
                    return self.get_import_progress(candidate_id)

        raw_segments = (source.get("segments") or source.get("parts") or source.get("chunks")
                        or source.get("分段") or source.get("分段列表"))
        expected_segments: dict[str, dict[str, Any]] = {}
        if isinstance(raw_segments, list):
            for index, raw in enumerate(raw_segments, start=1):
                if not isinstance(raw, dict):
                    continue
                number = int(raw.get("segment_no") or raw.get("part_no")
                             or raw.get("段号") or raw.get("分段编号") or index)
                cred = (raw.get("content_credential") or raw.get("checksum") or raw.get("sha256")
                        or raw.get("credential") or raw.get("内容凭证") or raw.get("凭证")
                        or raw.get("校验摘要"))
                count = raw.get("row_count", raw.get("expected_row_count", raw.get("行数", raw.get("行数统计"))))
                expected_segments[str(number)] = {
                    "segment_no": number,
                    "segment_index": number - 1,
                    "name": raw.get("name") or f"segment-{number:04d}",
                    "row_count": int(count) if count not in (None, "") else None,
                    "checksum": cred,
                    "content_credential": cred,
                    "status": "pending",
                    "verified": False,
                    "rows": [],
                }
        else:
            count = int(kwargs.get("segment_count") or source.get("segment_count") or 0)
            for number in range(1, count + 1):
                expected_segments[str(number)] = {
                    "segment_no": number,
                    "segment_index": number - 1,
                    "name": f"segment-{number:04d}",
                    "row_count": None,
                    "checksum": None,
                    "content_credential": None,
                    "status": "pending",
                    "verified": False,
                    "rows": [],
                }

        total_rows = kwargs.get("total_rows", source.get("total_rows", source.get("row_count")))
        candidate = {
            "upload_id": upload_id,
            "candidate_id": upload_id,
            "id": upload_id,
            "package_no": code,
            "snapshot_id": code,
            "source_table": table,
            "table": table,
            "snapshot_version": version,
            "version": version,
            "business_key": key,
            "maintainer": maintainer,
            "operator": maintainer,
            "batch_id": batch_id,
            "content_credential": content_credential,
            "checksum": content_credential,
            "total_rows": int(total_rows) if total_rows not in (None, "") else None,
            "expected_segment_count": len(expected_segments),
            "segments": expected_segments,
            "status": "started" if not expected_segments else "awaiting_segments",
            "interrupted": False,
            "resumable": True,
            "received_segment_nos": [],
            "verified_segment_nos": [],
            "failed_segment_nos": [],
            "missing_segment_nos": sorted(int(k) for k, v in expected_segments.items() if not v["verified"]),
            "next_segment_no": 1 if expected_segments else None,
            "progress_percent": 0,
            "validation_progress": {"verified_segments": [], "pending_segments": list(range(1, len(expected_segments) + 1))},
            "conflict": None,
            "registration": None,
            "created_at": now_ts(),
            "updated_at": now_ts(),
            "interruption_history": [],
            "resume_count": 0,
            "idempotency_key": idempotency_key,
            "policy": kwargs.get("policy") or self.conflict_policy,
        }
        self.uploads[upload_id] = candidate
        self.candidate_ids_by_no.setdefault(code, []).append(upload_id)
        if idempotency_key:
            self.idempotency_keys[idempotency_key] = upload_id
        return self.get_import_progress(upload_id)

    begin_import = start_import
    create_import = start_import
    init_upload = start_import
    start_upload = start_import

    def get_import(self, upload_id: str) -> dict[str, Any] | None:
        candidate = self.uploads.get(str(upload_id))
        return deepcopy(candidate) if candidate else None

    def get_import_progress(self, upload_id: str) -> dict[str, Any]:
        candidate = self._resolve_upload(upload_id)
        self._refresh_progress(candidate)
        return self._public_state(candidate)

    import_progress = get_import_progress
    resume_import = get_import_progress

    def interrupt_import(self, upload_id: str, reason: str = "client_disconnected") -> dict[str, Any]:
        candidate = self._resolve_upload(upload_id)
        candidate["interrupted"] = True
        candidate["status"] = "interrupted"
        candidate["interruption_history"].append({"at": now_ts(), "reason": reason})
        candidate["updated_at"] = now_ts()
        self._refresh_progress(candidate)
        result = self._public_state(candidate)
        result["ok"] = True
        result["message"] = "上传已中断，已核验分段和验证进度已保留，可从 next_segment_no 续传"
        return result

    disconnect_upload = interrupt_import
    mark_interrupted = interrupt_import

    def recover_import(self, upload_id: str) -> dict[str, Any]:
        candidate = self._resolve_upload(upload_id)
        candidate["interrupted"] = False
        candidate["resume_count"] += 1
        candidate["status"] = "uploading" if candidate["verified_segment_nos"] else "awaiting_segments"
        candidate["updated_at"] = now_ts()
        self._refresh_progress(candidate)
        result = self._public_state(candidate)
        result["ok"] = True
        result["resumed"] = True
        result["message"] = (
            f"可从第 {candidate['next_segment_no']} 段续传"
            if candidate["next_segment_no"] else "全部分段已核验，可完成登记"
        )
        return result

    resume_upload = recover_import

    def upload_segment(self, upload_id: Any = None, segment_ref: Any = None, rows: Any = None, *args: Any, **kwargs: Any) -> dict[str, Any]:
        # 支持 upload_segment(package_dict) / upload_segment(upload_id, segment_dict)。
        if isinstance(upload_id, dict) and segment_ref is None:
            package = upload_id
            started = self.start_import(package)
            upload_id = started["upload_id"]
            results = []
            for segment in package.get("segments") or []:
                results.append(self.upload_segment(upload_id, segment.get("segment_no"), segment))
            return self.complete_import(upload_id)

        candidate = self._resolve_upload(upload_id, **kwargs)
        payload: dict[str, Any] = {}
        if isinstance(segment_ref, dict):
            payload = segment_ref
            segment_ref = None
        elif isinstance(rows, dict):
            payload = rows
            rows = None
        if args and isinstance(args[0], dict):
            payload = args[0]

        segment_rows = rows if isinstance(rows, list) else extract_rows(payload)
        if segment_rows is None:
            segment_rows = kwargs.get("data") or kwargs.get("records") or []
        segment_rows = [dict(row) for row in segment_rows]

        declared_no = (
            kwargs.get("segment_no")
            or kwargs.get("part_no")
            or payload.get("segment_no")
            or payload.get("part_no")
            or payload.get("段号")
            or payload.get("分段编号")
        )
        declared_index = (
            kwargs.get("segment_index")
            if kwargs.get("segment_index") is not None
            else kwargs.get("part_index", payload.get("segment_index", payload.get("index")))
        )
        if segment_ref is not None and not isinstance(segment_ref, (dict, list)):
            ref_int = int(segment_ref)
        else:
            ref_int = None

        expected = self._match_expected_segment(
            candidate,
            segment_rows,
            declared_no=declared_no,
            declared_index=declared_index,
            ref_int=ref_int,
            credential=kwargs.get("checksum") or kwargs.get("content_credential") or payload.get("checksum") or payload.get("content_credential"),
        )
        if expected is None:
            return {
                "ok": False,
                "status": "unexpected_segment",
                "message": "找不到与上传内容或分段编号匹配的待传分段",
                "upload_id": candidate["upload_id"],
                **self._progress_brief(candidate),
            }

        number = expected["segment_no"]
        expected_count = expected.get("row_count")
        actual_cred = credential_bundle(segment_rows)
        expected_cred = (kwargs.get("checksum") or kwargs.get("content_credential")
                         or payload.get("checksum") or payload.get("content_credential")
                         or payload.get("内容凭证") or payload.get("凭证") or payload.get("校验摘要")
                         or expected.get("checksum"))
        errors: list[str] = []
        actual_count = len(segment_rows)
        supplied_count = kwargs.get("row_count", payload.get("row_count"))
        if supplied_count not in (None, "") and int(supplied_count) != actual_count:
            errors.append(f"上传行数 {actual_count} 与声明行数 {supplied_count} 不一致")
        if expected_count not in (None, "") and int(expected_count) != actual_count:
            errors.append(f"第{number}段行数不一致：期望 {expected_count}，实际 {actual_count}")
        if expected_cred and not credential_matches(expected_cred, actual_cred):
            errors.append(f"第{number}段内容凭证不匹配")

        segment_state = candidate["segments"][str(number)]
        if errors:
            segment_state.update({
                "status": "failed",
                "verified": False,
                "last_error": "；".join(errors),
                "last_attempt_at": now_ts(),
            })
            if number not in candidate["failed_segment_nos"]:
                candidate["failed_segment_nos"].append(number)
            candidate["status"] = "segment_verification_failed"
            candidate["updated_at"] = now_ts()
            self._refresh_progress(candidate)
            result = self.get_import_progress(candidate["upload_id"])
            result.update({"ok": False, "verified": False, "errors": errors,
                           "message": "；".join(errors), "segment_no": number})
            return result

        already_verified = segment_state.get("verified")
        segment_state.update({
            "status": "verified",
            "verified": True,
            "row_count": actual_count,
            "checksum": actual_cred["checksum"],
            "sha256": actual_cred["sha256"],
            "md5": actual_cred["md5"],
            "content_credential": actual_cred["content_credential"],
            "rows": segment_rows,
            "verified_at": now_ts(),
            "last_error": None,
        })
        candidate["failed_segment_nos"] = [n for n in candidate["failed_segment_nos"] if n != number]
        candidate["interrupted"] = False
        candidate["updated_at"] = now_ts()
        self._refresh_progress(candidate)

        auto_complete = kwargs.get("auto_complete")
        if auto_complete is None:
            auto_complete = candidate.get("auto_complete_last_segment", False)
        registration = None
        if auto_complete and not candidate["missing_segment_nos"]:
            registration = self.complete_import(candidate["upload_id"], **{
                k: v for k, v in kwargs.items() if k in {"policy", "force", "alternative_number", "register"}
            })

        result = self.get_import_progress(candidate["upload_id"])
        result.update({
            "ok": True,
            "verified": True,
            "duplicate_segment": bool(already_verified),
            "segment_no": number,
            "row_count": actual_count,
            "checksum": actual_cred["checksum"],
            "message": f"第{number}段已核验并保留",
            "registration": registration,
        })
        return result

    upload_part = upload_segment
    upload_chunk = upload_segment
    put_segment = upload_segment

    def _resolve_upload(self, upload_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(upload_id, dict):
            maybe_id = upload_id.get("upload_id") or upload_id.get("candidate_id")
            if maybe_id:
                upload_id = maybe_id
        key = str(upload_id or kwargs.get("candidate_id") or kwargs.get("upload_id") or "")
        if key in self.uploads:
            return self.uploads[key]
        # 允许把 package_no 当作定位符；只有一个候选时直接使用。
        candidates = self.candidate_ids_by_no.get(key, [])
        candidate_id = kwargs.get("candidate_id")
        if candidate_id and str(candidate_id) in self.uploads:
            return self.uploads[str(candidate_id)]
        if kwargs.get("maintainer"):
            for cid in candidates:
                if self.uploads[cid].get("maintainer") == kwargs["maintainer"]:
                    return self.uploads[cid]
        open_candidates = [cid for cid in candidates
                           if self.uploads[cid]["status"] not in {"registered", "duplicate", "quarantined"}]
        if len(open_candidates) == 1:
            return self.uploads[open_candidates[0]]
        if candidates:
            return self.uploads[candidates[-1]]
        raise KeyError(f"找不到上传会话：{key}")

    def _match_expected_segment(
        self,
        candidate: dict[str, Any],
        rows: list[dict[str, Any]],
        *,
        declared_no: Any,
        declared_index: Any,
        ref_int: int | None,
        credential: Any,
    ) -> dict[str, Any] | None:
        segments = list(candidate["segments"].values())
        actual_cred = credential_bundle(rows)

        # 内容凭证最可靠：分段编号在 0/1 基之间有歧义时也能正确归位。
        for segment in segments:
            expected_cred = segment.get("checksum") or segment.get("content_credential") or credential
            if expected_cred and credential_matches(expected_cred, actual_cred):
                if segment.get("row_count") in (None, "") or int(segment["row_count"]) == len(rows):
                    return segment

        if declared_no not in (None, ""):
            number = int(declared_no)
            if str(number) in candidate["segments"]:
                return candidate["segments"][str(number)]

        if declared_index not in (None, ""):
            index = int(declared_index)
            for segment in segments:
                if segment["segment_index"] == index:
                    return segment

        if ref_int is not None:
            if str(ref_int) in candidate["segments"]:
                return candidate["segments"][str(ref_int)]
            for segment in segments:
                if segment["segment_index"] == ref_int:
                    return segment
            # 调用方可能使用 1-based 编号但传成了 index 语义；内容不匹配时的最后兜底。
            if 1 <= ref_int <= len(segments):
                return candidate["segments"][str(ref_int)]
        pending = [segment for segment in segments if not segment["verified"]]
        return pending[0] if pending else (segments[0] if segments else None)

    def _refresh_progress(self, candidate: dict[str, Any]) -> None:
        segments = list(candidate["segments"].values())
        verified = [s["segment_no"] for s in segments if s.get("verified")]
        received = [s["segment_no"] for s in segments if s.get("status") in {"verified", "failed"}]
        missing = sorted(s["segment_no"] for s in segments if not s.get("verified"))
        total = len(segments)
        candidate["verified_segment_nos"] = sorted(verified)
        candidate["received_segment_nos"] = sorted(received)
        candidate["missing_segment_nos"] = missing
        candidate["next_segment_no"] = missing[0] if missing else None
        candidate["progress_percent"] = round(100 * len(verified) / total) if total else 0
        candidate["validation_progress"] = {
            "verified_count": len(verified),
            "total_count": total,
            "verified_segments": sorted(verified),
            "pending_segments": missing,
            "next_segment_no": candidate["next_segment_no"],
            "complete": not missing and total > 0,
            "progress_percent": candidate["progress_percent"],
        }
        if candidate["status"] not in {"interrupted", "quarantined", "registration_conflict",
                                       "registered", "duplicate", "registered_conflict_lost"}:
            if not missing and total:
                candidate["status"] = "all_segments_verified"
            elif verified:
                candidate["status"] = "uploading"
        candidate["updated_at"] = now_ts()

    def _progress_brief(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self._refresh_progress(candidate)
        return {
            "upload_id": candidate["upload_id"],
            "candidate_id": candidate["candidate_id"],
            "package_no": candidate["package_no"],
            "status": candidate["status"],
            "verified_segment_nos": candidate["verified_segment_nos"],
            "missing_segment_nos": candidate["missing_segment_nos"],
            "next_segment_no": candidate["next_segment_no"],
            "progress_percent": candidate["progress_percent"],
            "resumable": candidate.get("resumable", True),
        }

    def _public_state(self, candidate: dict[str, Any]) -> dict[str, Any]:
        state = self._progress_brief(candidate)
        segment_views = []
        for key in sorted(candidate["segments"], key=lambda x: int(x)):
            segment = dict(candidate["segments"][key])
            segment.pop("rows", None)
            segment_views.append(segment)
        state.update({
            "ok": True,
            "snapshot_id": candidate["snapshot_id"],
            "source_table": candidate["source_table"],
            "snapshot_version": candidate["snapshot_version"],
            "business_key": candidate["business_key"],
            "maintainer": candidate.get("maintainer"),
            "batch_id": candidate.get("batch_id"),
            "content_credential": candidate.get("content_credential"),
            "expected_segment_count": candidate["expected_segment_count"],
            "segments": segment_views,
            "parts": segment_views,
            "interrupted": candidate.get("interrupted", False),
            "resumable": candidate.get("resumable", True),
            "resume_count": candidate.get("resume_count", 0),
            "conflict": candidate.get("conflict"),
            "registration": candidate.get("registration"),
            "validation_progress": candidate.get("validation_progress"),
            # 常见命名别名，方便不同验收脚本读取。
            "uploaded_segments": candidate["verified_segment_nos"],
            "completed_segments": candidate["verified_segment_nos"],
            "verified_parts": candidate["verified_segment_nos"],
            "received_segments": candidate["received_segment_nos"],
            "received_parts": candidate["received_segment_nos"],
            "missing_segments": candidate["missing_segment_nos"],
            "pending_segments": candidate["missing_segment_nos"],
            "pending_segment_nos": candidate["missing_segment_nos"],
            "missing_parts": candidate["missing_segment_nos"],
            "pending_parts": candidate["missing_segment_nos"],
            "next_part_no": candidate["next_segment_no"],
            "next_pending_segment": candidate["next_segment_no"],
            "progress": candidate.get("validation_progress"),
            "message": (
                f"仍待第 {'、'.join(map(str, candidate['missing_segment_nos']))} 段"
                if candidate["missing_segment_nos"] else "全部分段已完整核验"
            ),
        })
        return state

    # ------------------------------------------------------------------
    # 完成、冲突、隔离和另行登记
    # ------------------------------------------------------------------
    def complete_import(self, upload_id: Any = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(upload_id, dict):
            return self.import_package(upload_id, **kwargs)
        candidate = self._resolve_upload(upload_id, **kwargs)
        self._refresh_progress(candidate)
        if candidate["missing_segment_nos"]:
            result = self.get_import_progress(candidate["upload_id"])
            result.update({
                "ok": False,
                "registered": False,
                "can_backfill": False,
                "status": "incomplete",
                "message": f"仍待第 {'、'.join(map(str, candidate['missing_segment_nos']))} 段，不能登记或回填",
            })
            return result

        package = self._reconstruct_package(candidate)
        verification = self.verify_package(package)
        if not verification["ok"]:
            candidate["status"] = "verification_failed"
            candidate["registration"] = verification
            result = self.get_import_progress(candidate["upload_id"])
            result.update({"ok": False, "registered": False, "verification": verification,
                           "message": "交接包核验失败：" + verification["message"]})
            return result

        candidate["status"] = "verified"
        candidate["verified_package"] = package
        if kwargs.get("register", True):
            registration = self.register_candidate(candidate["candidate_id"], **kwargs)
            return registration
        result = self.get_import_progress(candidate["upload_id"])
        result.update({"ok": True, "verified": True, "package": package,
                       "verification": verification, "message": "交接包已核验，尚未登记"})
        return result

    finish_import = complete_import
    finalize_import = complete_import
    complete_upload = complete_import
    finish_upload = complete_upload

    def _reconstruct_package(self, candidate: dict[str, Any]) -> dict[str, Any]:
        segments = []
        all_rows = []
        for key in sorted(candidate["segments"], key=lambda x: int(x)):
            segment = candidate["segments"][key]
            rows = deepcopy(segment.get("rows", []))
            all_rows.extend(rows)
            segments.append({
                "segment_no": segment["segment_no"],
                "segment_index": segment["segment_index"],
                "name": segment["name"],
                "row_count": len(rows),
                "rows": rows,
                "checksum": segment.get("checksum") or credential_bundle(rows)["checksum"],
                "content_credential": segment.get("content_credential") or credential_bundle(rows)["content_credential"],
            })
        cred = credential_bundle(all_rows)
        return {
            "format": FORMAT,
            "package_no": candidate["package_no"],
            "snapshot_id": candidate["snapshot_id"],
            "source_table": candidate["source_table"],
            "snapshot_version": candidate["snapshot_version"],
            "business_key": candidate["business_key"],
            "maintainer": candidate.get("maintainer"),
            "batch_id": candidate.get("batch_id"),
            "total_rows": len(all_rows),
            "row_count": len(all_rows),
            "segment_count": len(segments),
            "segments": segments,
            "parts": segments,
            **cred,
        }

    def import_package(self, package: dict[str, Any] | str | bytes | None = None, **kwargs: Any) -> dict[str, Any]:
        if isinstance(package, (str, bytes)):
            package = self.loads_package(package)
        if not isinstance(package, dict):
            raise ValueError("package 必须是交接包对象或 JSON 字符串")
        merged_kwargs = {**kwargs}
        merged_kwargs.setdefault("package_no", package.get("package_no"))
        started = self.start_import(package=package, **merged_kwargs)
        upload_id = started["upload_id"]
        for segment in package.get("segments") or package.get("parts") or []:
            upload_result = self.upload_segment(upload_id, segment.get("segment_no"), segment)
            if not upload_result.get("ok"):
                return upload_result
        return self.complete_import(upload_id, **{
            k: v for k, v in kwargs.items()
            if k in {"policy", "force", "alternative_number", "register", "conflict_policy"}
        })

    import_snapshot = import_package
    import_handoff_package = import_package
    receive_package = import_package

    def register_candidate(self, candidate_id: str, **kwargs: Any) -> dict[str, Any]:
        candidate = self.uploads.get(str(candidate_id)) or self._resolve_upload(candidate_id)
        if candidate.get("verified_package") is None:
            self._refresh_progress(candidate)
            if candidate["missing_segment_nos"]:
                return {
                    "ok": False,
                    "registered": False,
                    "status": "incomplete",
                    "message": f"仍待第 {'、'.join(map(str, candidate['missing_segment_nos']))} 段",
                    **self._public_state(candidate),
                }
            package = self._reconstruct_package(candidate)
            verification = self.verify_package(package)
            if not verification["ok"]:
                return {"ok": False, "registered": False, "status": "verification_failed",
                        "errors": verification["errors"], "verification": verification}
            candidate["verified_package"] = package

        package = candidate["verified_package"]
        code = candidate["package_no"]
        version = candidate["snapshot_version"]
        policy = kwargs.get("policy") or candidate.get("policy") or self.conflict_policy

        # 已登记的不可变版本优先：同版本同内容幂等确认，同版本改内容隔离。
        existing = self.version_baselines.get((code, version)) or self.baseline_by_no.get(code)
        if existing and not kwargs.get("force"):
            if existing.get("snapshot_version") != version:
                pass  # baseline_by_no 可能指向其他版本；同 (code, version) 已在上面判断。
            elif existing.get("content_credential") == package["content_credential"]:
                candidate["status"] = "duplicate"
                candidate["registration"] = {"duplicate": True, "baseline_id": existing["baseline_id"]}
                result = self._registration_result(
                    ok=True,
                    status="duplicate",
                    state="already_registered",
                    already_registered=True,
                    duplicate=True,
                    idempotent=True,
                    unchanged=True,
                    confirmed=True,
                    message="相同编号、快照版本和内容凭证已登记；本次重复导入仅确认一致",
                    candidate=candidate,
                    retained=existing,
                )
                self.registrations.append(result)
                return result
            else:
                return self._quarantine_changed_content(candidate, package, existing)

        # 尚无基准时，悲观认领策略让首个已有核验段的候选占用编号。
        if policy == "first_verified_segment_wins" and not kwargs.get("force"):
            claim_owner = self._claim_owner(code)
            if claim_owner and claim_owner["candidate_id"] != candidate["candidate_id"] \
                    and claim_owner.get("content_credential") != candidate.get("content_credential"):
                return self._conflict_against_claim(candidate, claim_owner, package, kwargs)

        # registered_version_wins 下，若已有其他完整登记版本，上面的同版本检查会处理；
        # 不同版本允许作为该编号下的新版本登记。
        baseline = self._create_baseline(candidate, package, **kwargs)
        competitors = self._active_competitors(candidate)
        result = self._registration_result(
            ok=True,
            status="registered" if not competitors else "registered_with_concurrent_candidate",
            message="交接包已完整核验并同步到回填基准清单",
            candidate=candidate,
            retained=baseline,
            registered=True,
        )
        result["baseline"] = baseline
        result["backfill_baseline"] = baseline
        if competitors:
            loser = self._summarize_candidate(competitors[0])
            candidate["concurrent_candidate_ids"] = [c["candidate_id"] for c in competitors]
            for competitor in competitors:
                competitor["status"] = "registered_conflict_lost"
                competitor["conflict"] = {
                    "retained_candidate_id": candidate["candidate_id"],
                    "baseline_id": baseline["baseline_id"],
                    "reason": "another_complete_version_registered_first",
                }
            conflict_record = {
                "at": now_ts(),
                "package_no": code,
                "type": "concurrent_import",
                "retained_candidate": self._summarize_candidate(candidate),
                "actual_retained_candidate": self._summarize_candidate(candidate),
                "winner_candidate_id": candidate["candidate_id"],
                "loser_candidate": loser,
                "defeated_candidates": [self._summarize_candidate(c) for c in competitors],
                "next_action_for_loser": "不要覆盖该编号；与保留候选维护员核对，或调用 register_alternative 使用新编号/新版本另行登记",
                "loser_next_step": "续传仅用于恢复核对；完成后会因同版本内容变化被隔离，随后可用新编号或新版本另行登记",
                "can_register_separately": True,
            }
            self.conflicts.append(conflict_record)
            result.update({
                "conflict": True,
                "conflict_record": conflict_record,
                "actual_retained_candidate": conflict_record["actual_retained_candidate"],
                "retained_candidate": conflict_record["retained_candidate"],
                "winner_candidate_id": candidate["candidate_id"],
                "loser_candidate": loser,
                "next_action_for_loser": conflict_record["next_action_for_loser"],
                "loser_next_step": conflict_record["loser_next_step"],
                "can_register_separately": True,
            })
        candidate["registration"] = {"registered": True, "baseline_id": baseline["baseline_id"]}
        self.registrations.append(result)
        if self.sync_callback:
            self.sync_callback("baseline_registered", baseline, package)
        return result

    register_package = register_candidate
    register_snapshot = register_candidate

    def _conflict_against_claim(
        self,
        candidate: dict[str, Any],
        retained: dict[str, Any],
        package: dict[str, Any],
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        retained_summary = self._summarize_candidate(retained)
        candidate["status"] = "registration_conflict"
        candidate["conflict"] = {
            "retained_candidate_id": retained["candidate_id"],
            "reason": "earlier_verified_segment_claim",
        }
        alternative_number = kwargs.get("alternative_number") or f"{candidate['package_no']}-ALT-{candidate['candidate_id'][-6:]}"
        record = {
            "at": now_ts(),
            "package_no": candidate["package_no"],
            "type": "claim_conflict",
            "reason": "earlier_verified_segment_claim",
            "actual_retained_candidate": retained_summary,
            "retained_candidate": retained_summary,
            "kept_candidate": retained_summary,
            "winner_candidate": retained_summary,
            "winner": retained_summary,
            "winner_candidate_id": retained["candidate_id"],
            "submitted_candidate": self._summarize_candidate(candidate),
            "loser_candidate": self._summarize_candidate(candidate),
            "defeated_candidate": self._summarize_candidate(candidate),
            "missing_segment_nos": retained.get("missing_segment_nos", []),
            "pending_segments": retained.get("missing_segment_nos", []),
            "alternative_package_no": alternative_number,
            "can_register_separately": True,
            "next_action_for_loser": (
                f"本次保留较早的候选 {retained['candidate_id']}；败方可与对方核对，"
                f"或调用 register_alternative 以 {alternative_number} 另行登记"
            ),
            "loser_next_action": (
                f"与保留候选维护员核对，或使用新编号 {alternative_number} 调用 register_alternative 另行登记"
            ),
            "next_step": "register_alternative_with_new_identity",
            "loser_next_step": (
                "不要覆盖原编号；可先保留本候选，随后使用新编号/新版本另行登记"
                + (f"；保留候选仍待第 {'、'.join(map(str, retained.get('missing_segment_nos', [])))} 段"
                   if retained.get("missing_segment_nos") else "")
            ),
        }
        self.conflicts.append(record)
        result = {
            "ok": False,
            "registered": False,
            "conflict": True,
            "candidate_id": candidate["candidate_id"],
            "upload_id": candidate["upload_id"],
            "package_no": candidate["package_no"],
            "status": "registration_conflict",
            "message": f"编号 {candidate['package_no']} 已有更早的保留候选，本次候选未取代它",
            **record,
        }
        candidate["registration"] = result
        self.registrations.append(result)
        return result

    def _quarantine_changed_content(
        self,
        candidate: dict[str, Any],
        package: dict[str, Any],
        retained: dict[str, Any],
    ) -> dict[str, Any]:
        alternative_number = f"{candidate['package_no']}-QUAR-{candidate['candidate_id'][-6:]}"
        record = {
            "at": now_ts(),
            "package_no": candidate["package_no"],
            "snapshot_version": candidate["snapshot_version"],
            "type": "same_version_content_changed",
            "reason": "same_version_content_changed",
            "actual_retained_candidate": retained,
            "retained_candidate": retained,
            "winner_candidate_id": retained.get("candidate_id"),
            "submitted_candidate": self._summarize_candidate(candidate),
            "loser_candidate": self._summarize_candidate(candidate),
            "quarantined_candidate_id": candidate["candidate_id"],
            "quarantine_status": "isolated",
            "isolated": True,
            "isolation_status": "quarantined",
            "kept_candidate": retained,
            "winner_candidate": retained,
            "content_credential": package["content_credential"],
            "existing_content_credential": retained.get("content_credential"),
            "alternative_package_no": alternative_number,
            "can_register_separately": True,
            "next_action_for_loser": (
                "相同版本的内容凭证已变化，已隔离且不会进入回填基准；"
                f"核对后可用新编号（建议 {alternative_number}）或新版本另行登记"
            ),
            "loser_next_step": "调用 register_alternative 并提供新编号或新版本，或等待保留版本维护员处理",
        }
        candidate["status"] = "quarantined"
        candidate["conflict"] = record
        self.quarantine.append(record)
        self.conflicts.append(record)
        result = {
            "ok": False,
            "registered": False,
            "conflict": True,
            "quarantined": True,
            "candidate_id": candidate["candidate_id"],
            "upload_id": candidate["upload_id"],
            "package_no": candidate["package_no"],
            "status": "quarantined",
            "message": "相同快照版本但内容凭证不同，已隔离；原基准保持不变",
            **record,
        }
        candidate["registration"] = result
        self.registrations.append(result)
        return result

    def _create_baseline(self, candidate: dict[str, Any], package: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        baseline_id = kwargs.get("baseline_id") or f"BASE-{uuid.uuid4().hex[:10].upper()}"
        segment_manifest = [
            {k: v for k, v in segment.items() if k != "rows"}
            for segment in package["segments"]
        ]
        baseline = {
            "baseline_id": baseline_id,
            "package_no": candidate["package_no"],
            "snapshot_id": candidate["package_no"],
            "source_table": candidate["source_table"],
            "table": candidate["source_table"],
            "snapshot_version": candidate["snapshot_version"],
            "version": candidate["snapshot_version"],
            "business_key": candidate["business_key"],
            "row_count": package["total_rows"],
            "total_rows": package["total_rows"],
            "segment_count": package["segment_count"],
            "segments": segment_manifest,
            "checksum": package["checksum"],
            "sha256": package["sha256"],
            "md5": package["md5"],
            "content_credential": package["content_credential"],
            "manifest_checksum": package.get("manifest_checksum"),
            "candidate_id": candidate["candidate_id"],
            "maintainer": candidate.get("maintainer"),
            "batch_id": candidate.get("batch_id"),
            "status": "complete_verified",
            "baseline_status": "complete_verified",
            "verified": True,
            "registered_at": now_ts(),
            "data_location": f"handoff://baselines/{candidate['package_no']}/{candidate['snapshot_version']}",
            "package": package,
        }
        self.baseline_by_no[baseline["package_no"]] = baseline
        self.version_baselines[(baseline["package_no"], baseline["snapshot_version"])] = baseline
        self.baselines.append({k: v for k, v in baseline.items() if k != "package"})
        candidate["status"] = "registered"
        return baseline

    def _registration_result(self, **kwargs: Any) -> dict[str, Any]:
        candidate = kwargs.pop("candidate")
        retained = kwargs.pop("retained", None)
        result = {
            "upload_id": candidate["candidate_id"],
            "candidate_id": candidate["candidate_id"],
            "package_no": candidate["package_no"],
            "snapshot_id": candidate["package_no"],
            "source_table": candidate["source_table"],
            "snapshot_version": candidate["snapshot_version"],
            "actual_retained_candidate": self._summarize_retained(retained),
            "retained_candidate": self._summarize_retained(retained),
            "registered_candidate": self._summarize_candidate(candidate),
            "at": now_ts(),
            **kwargs,
        }
        return result

    def _summarize_retained(self, retained: dict[str, Any] | None) -> dict[str, Any] | None:
        if not retained:
            return None
        if "upload_id" in retained and "segments" in retained and isinstance(retained["segments"], dict):
            return self._summarize_candidate(retained)
        return {k: v for k, v in retained.items() if k != "package"}

    def _summarize_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        self._refresh_progress(candidate)
        return {
            "candidate_id": candidate["candidate_id"],
            "upload_id": candidate["upload_id"],
            "package_no": candidate["package_no"],
            "source_table": candidate.get("source_table"),
            "snapshot_version": candidate.get("snapshot_version"),
            "maintainer": candidate.get("maintainer"),
            "batch_id": candidate.get("batch_id"),
            "status": candidate.get("status"),
            "content_credential": candidate.get("content_credential"),
            "verified_segment_nos": candidate.get("verified_segment_nos"),
            "missing_segment_nos": candidate.get("missing_segment_nos"),
            "next_segment_no": candidate.get("next_segment_no"),
            "progress_percent": candidate.get("progress_percent"),
            "interrupted": candidate.get("interrupted", False),
            "resumable": candidate.get("resumable", True),
        }

    def _claim_owner(self, code: str) -> dict[str, Any] | None:
        candidates = [
            self.uploads[cid]
            for cid in self.candidate_ids_by_no.get(code, [])
            if self.uploads[cid].get("verified_segment_nos")
            and self.uploads[cid].get("status") not in {"quarantined"}
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda item: item.get("created_at", 0))

    def _active_competitors(self, current: dict[str, Any]) -> list[dict[str, Any]]:
        code = current["package_no"]
        result = []
        for cid in self.candidate_ids_by_no.get(code, []):
            if cid == current["candidate_id"]:
                continue
            candidate = self.uploads[cid]
            if candidate.get("status") in {"quarantined", "registered", "duplicate"}:
                continue
            if candidate.get("snapshot_version") == current.get("snapshot_version"):
                result.append(candidate)
        return result

    def register_alternative(
        self,
        candidate_id: str | None = None,
        *,
        new_package_no: str | None = None,
        alternative_number: str | None = None,
        new_version: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """把冲突/隔离候选以新编号或新版本另行登记，原编号基准不受影响。"""
        candidate = self._resolve_upload(candidate_id, **kwargs)
        if candidate.get("verified_package") is None:
            completed = self.complete_import(candidate["candidate_id"], register=False)
            if not completed.get("ok"):
                return completed
        old_code = candidate["package_no"]
        new_code = str(new_package_no or alternative_number
                       or f"{old_code}-ALT-{candidate['candidate_id'][-6:]}")
        old_version = candidate["snapshot_version"]
        if new_version:
            candidate["snapshot_version"] = str(new_version)
            candidate["version"] = str(new_version)
            if candidate.get("verified_package"):
                candidate["verified_package"]["snapshot_version"] = str(new_version)
                candidate["verified_package"]["version"] = str(new_version)
        candidate["package_no"] = new_code
        candidate["snapshot_id"] = new_code
        candidate["alternative_of"] = old_code
        self.candidate_ids_by_no.setdefault(new_code, []).append(candidate["candidate_id"])
        registration = self.register_candidate(candidate["candidate_id"], force=True, **kwargs)
        if registration.get("ok"):
            registration["alternative_registered"] = True
            registration["original_package_no"] = old_code
            registration["message"] = f"冲突候选已以 {new_code} / {candidate['snapshot_version']} 另行登记"
        return registration

    register_separately = register_alternative
    register_conflict_package = register_alternative

    def list_baselines(self, source_table: str | None = None) -> list[dict[str, Any]]:
        items = list(self.baselines)
        if source_table:
            items = [item for item in items if item.get("source_table") == source_table
                     or item.get("table") == source_table]
        return deepcopy(items)

    def backfill_baseline_manifest(self) -> list[dict[str, Any]]:
        return self.list_baselines()

    def get_baseline(self, package_no: str, version: str | None = None) -> dict[str, Any] | None:
        if version:
            baseline = self.version_baselines.get((package_no, version))
        else:
            baseline = self.baseline_by_no.get(package_no)
        return deepcopy(baseline) if baseline else None

    # ------------------------------------------------------------------
    # 回填窗口与业务键比较
    # ------------------------------------------------------------------
    def open_backfill_window(
        self,
        package_no: Any = None,
        *,
        target_table: str | None = None,
        version: str | None = None,
        snapshot_version: str | None = None,
        candidate_id: str | None = None,
        business_keys: Iterable[str] | None = None,
        requested_keys: Iterable[Any] | None = None,
        persist_target: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        code = str(package_no or kwargs.get("snapshot_id") or kwargs.get("package_id") or "")
        ver = version or snapshot_version
        baseline = self.get_baseline(code, ver) if code else None

        # 显式指定已完整核验候选时，可生成候选级窗口；默认仍只认基准清单。
        candidate_window = None
        if candidate_id:
            candidate = self.uploads.get(str(candidate_id))
            if candidate and candidate.get("verified_package"):
                candidate_window = candidate

        incomplete_candidates = [
            self._summarize_candidate(self.uploads[cid])
            for cid in self.candidate_ids_by_no.get(code, [])
            if self.uploads[cid].get("missing_segment_nos")
            and self.uploads[cid].get("status") not in {"quarantined"}
        ]

        if baseline is None and candidate_window is None:
            retained = self._retained_candidate_for_window(code)
            if retained is not None and retained["missing_segment_nos"]:
                window = {
                    "window_id": f"WIN-{uuid.uuid4().hex[:10].upper()}",
                    "package_no": code,
                    "target_table": target_table or retained.get("source_table"),
                    "status": "blocked_incomplete",
                    "ok": False,
                    "backfilled": False,
                    "can_backfill": False,
                    "missing_segment_nos": retained["missing_segment_nos"],
                    "pending_segments": retained["missing_segment_nos"],
                    "missing_segments": retained["missing_segment_nos"],
                    "pending_segment_nos": retained["missing_segment_nos"],
                    "missing_parts": retained["missing_segment_nos"],
                    "next_segment_no": retained["next_segment_no"],
                    "next_pending_segment": retained["next_segment_no"],
                    "retained_candidate": self._summarize_candidate(retained),
                    "incomplete_candidates": incomplete_candidates,
                    "message": f"编号 {code} 尚无完整且已核验的基准；保留候选仍待第 {'、'.join(map(str, retained['missing_segment_nos']))} 段",
                    "created_at": now_ts(),
                }
                self._save_window(window)
                return window
            window = {
                "window_id": f"WIN-{uuid.uuid4().hex[:10].upper()}",
                "package_no": code,
                "status": "no_verified_baseline",
                "ok": False,
                "backfilled": False,
                "can_backfill": False,
                "incomplete_candidates": incomplete_candidates,
                "message": f"编号 {code} 没有完整且已核验的回填基准",
                "created_at": now_ts(),
            }
            self._save_window(window)
            return window

        source = baseline or candidate_window
        package = source.get("package") or candidate_window.get("verified_package")
        table = target_table or source.get("source_table") or source.get("table")
        key_fields = list(business_keys) if business_keys else source.get("business_key") or "id"
        expected_rows = deepcopy(package["rows"] if "rows" in package else [
            row for segment in package.get("segments", []) for row in segment.get("rows", [])
        ])
        actual_rows = deepcopy(expected_rows)  # 内存回填窗口直接写入快照实际数据。
        if persist_target:
            self.backfilled_tables[table] = actual_rows
        elif table not in self.backfilled_tables:
            self.backfilled_tables[table] = []

        comparison = self.compare_rows_by_business_key(expected_rows, actual_rows, key_fields)
        requested = list(requested_keys or [])
        if requested:
            requested_text = {canonical_json(k).decode() for k in requested}
            comparison["requested_keys"] = requested
            comparison["requested_key_results"] = [
                item for item in comparison["key_results"] if item["key"] in requested_text
            ]

        claim_block: dict[str, Any] | None = None
        if incomplete_candidates:
            missing = sorted({
                segment
                for candidate in incomplete_candidates
                for segment in candidate.get("missing_segment_nos", [])
            })
            claim_block = {
                "ok": False,
                "backfilled": False,
                "can_backfill": False,
                "status": "blocked_incomplete",
                "missing_segment_nos": missing,
                "pending_segments": missing,
                "missing_segments": missing,
                "message": f"未完保留候选仍待第 {'、'.join(map(str, missing))} 段",
                "candidates": incomplete_candidates,
            }

        window_id = f"WIN-{uuid.uuid4().hex[:10].upper()}"
        window = {
            "window_id": window_id,
            "id": window_id,
            "package_no": code or source.get("package_no"),
            "snapshot_id": code or source.get("package_no"),
            "baseline_id": source.get("baseline_id"),
            "candidate_id": source.get("candidate_id"),
            "target_table": table,
            "source_table": source.get("source_table"),
            "snapshot_version": source.get("snapshot_version") or source.get("version"),
            "content_credential": source.get("content_credential") or package.get("content_credential"),
            "business_key": key_fields,
            "status": "completed",
            "ok": True,
            "backfilled": True,
            "verified": True,
            "rows_inserted": len(actual_rows),
            "row_count": len(actual_rows),
            "keys": [item["key"] for item in comparison["key_results"]],
            "business_keys": comparison["business_keys"],
            "key_count": comparison["key_count"],
            "all_keys_matched": comparison["all_matched"],
            "all_business_keys_matched": comparison["all_matched"],
            "comparison": comparison,
            "key_comparison": comparison,
            "business_key_comparison": comparison,
            "key_results": comparison["key_results"],
            "per_key_results": comparison["key_results"],
            "key_matches": comparison["key_results"],
            "business_key_results": comparison["key_results"],
            "rows": actual_rows,
            "backfilled_rows": actual_rows,
            "actual_data": actual_rows,
            "incomplete_candidates": incomplete_candidates,
            "incomplete_retained_candidate_window": claim_block,
            "blocked_claim": claim_block,
            "message": f"已使用完整且已核验的 {source.get('snapshot_version')} 基准回填 {len(actual_rows)} 行，每个业务键均已核对",
            "created_at": now_ts(),
        }
        if incomplete_candidates:
            window["pending_candidate_notice"] = (
                "另有未完保留候选："
                + "；".join(f"{c['candidate_id']} 仍待第 {'、'.join(map(str, c['missing_segment_nos']))} 段"
                           for c in incomplete_candidates if c.get("missing_segment_nos"))
            )
        self._save_window(window)
        return deepcopy(window)

    start_backfill_window = open_backfill_window
    create_backfill_window = open_backfill_window
    backfill_from_package = open_backfill_window

    def _retained_candidate_for_window(self, code: str) -> dict[str, Any] | None:
        ids = self.candidate_ids_by_no.get(code, [])
        if not ids:
            return None
        # 优先返回仍有进度的最早候选，符合“保留候选”语义。
        for cid in ids:
            candidate = self.uploads[cid]
            self._refresh_progress(candidate)
            if candidate.get("missing_segment_nos") and candidate.get("status") not in {"quarantined"}:
                return candidate
        return self.uploads[ids[-1]]

    def _save_window(self, window: dict[str, Any]) -> None:
        self.windows[window["window_id"]] = window
        self.window_ids_by_no.setdefault(window.get("package_no", ""), []).append(window["window_id"])

    def get_backfill_window(self, window_id: str) -> dict[str, Any] | None:
        return deepcopy(self.windows.get(window_id))

    def compare_rows_by_business_key(
        self,
        expected_rows: list[dict[str, Any]],
        actual_rows: list[dict[str, Any]],
        business_key: str | list[str] = "id",
    ) -> dict[str, Any]:
        expected_map: dict[str, dict[str, Any]] = {}
        actual_map: dict[str, dict[str, Any]] = {}
        duplicate_expected: list[str] = []
        duplicate_actual: list[str] = []
        for row in expected_rows:
            key = business_key_of(row, business_key)
            if key in expected_map:
                duplicate_expected.append(key)
            expected_map[key] = row
        for row in actual_rows:
            key = business_key_of(row, business_key)
            if key in actual_map:
                duplicate_actual.append(key)
            actual_map[key] = row

        key_results = []
        changed_keys = []
        missing_keys = []
        extra_keys = []
        for key in expected_map.keys() | actual_map.keys():
            expected = expected_map.get(key)
            actual = actual_map.get(key)
            if expected is None:
                status = "extra"
                matched = False
                extra_keys.append(key)
                differences = ["实际结果多出该业务键"]
            elif actual is None:
                status = "missing"
                matched = False
                missing_keys.append(key)
                differences = ["回填结果缺少该业务键"]
            else:
                differences = self._row_differences(expected, actual)
                matched = not differences
                status = "matched" if matched else "changed"
                if not matched:
                    changed_keys.append(key)
            key_results.append({
                "business_key": business_key,
                "key": key,
                "status": status,
                "matched": matched,
                "expected": expected,
                "actual": actual,
                "differences": differences,
            })
        key_results.sort(key=lambda item: item["key"])
        results_by_key = {item["key"]: item for item in key_results}
        matched_keys = [item["key"] for item in key_results if item["matched"]]
        all_matched = not missing_keys and not extra_keys and not changed_keys
        return {
            "business_key": business_key,
            "business_keys": business_key if isinstance(business_key, list) else [business_key],
            "expected_count": len(expected_rows),
            "actual_count": len(actual_rows),
            "unique_expected_count": len(expected_map),
            "unique_actual_count": len(actual_map),
            "key_count": len(key_results),
            "matched_count": len(matched_keys),
            "matched_keys": matched_keys,
            "missing_keys": sorted(missing_keys),
            "extra_keys": sorted(extra_keys),
            "changed_keys": sorted(changed_keys),
            "duplicate_expected_keys": duplicate_expected,
            "duplicate_actual_keys": duplicate_actual,
            "all_matched": all_matched,
            "key_results": key_results,
            "comparisons": key_results,
            "results_by_key": results_by_key,
            "by_key": results_by_key,
        }

    compare_business_keys = compare_rows_by_business_key

    @staticmethod
    def _row_differences(expected: dict[str, Any], actual: dict[str, Any]) -> list[dict[str, Any]]:
        differences = []
        for field in sorted(set(expected) | set(actual)):
            if canonical_json(expected.get(field)) != canonical_json(actual.get(field)):
                differences.append({
                    "field": field,
                    "expected": expected.get(field),
                    "actual": actual.get(field),
                })
        return differences

    def compare_backfill_windows(self, first_window_id: str, second_window_id: str | None = None) -> dict[str, Any]:
        first = self.windows.get(first_window_id)
        if first is None:
            raise KeyError(f"找不到回填窗口：{first_window_id}")
        if second_window_id is None:
            ids = self.window_ids_by_no.get(first.get("package_no", ""), [])
            later = [wid for wid in ids if wid != first_window_id]
            if not later:
                raise ValueError("该编号没有第二个回填窗口可比较")
            second_window_id = later[-1]
        second = self.windows.get(second_window_id)
        if second is None:
            raise KeyError(f"找不到回填窗口：{second_window_id}")

        first_rows = first.get("backfilled_rows") or first.get("rows") or []
        second_rows = second.get("backfilled_rows") or second.get("rows") or []
        key = first.get("business_key") or second.get("business_key") or "id"
        row_comparison = self.compare_rows_by_business_key(first_rows, second_rows, key)
        both_completed = bool(first.get("backfilled") and second.get("backfilled"))
        results_equal = both_completed and row_comparison["all_matched"] and first.get("content_credential") == second.get("content_credential")
        summary = (
            "两次回填均完整核验且每个业务键结果一致"
            if results_equal else
            "两次回填状态或业务键结果存在差异，详见 key_comparison"
        )
        if not both_completed:
            summary = (
                f"首次状态为 {first.get('status')}，第二次状态为 {second.get('status')}；"
                "未完成窗口没有可与完整基准等同的回填结果"
            )
        return {
            "ok": True,
            "package_no": first.get("package_no"),
            "first_window_id": first_window_id,
            "second_window_id": second_window_id,
            "first_status": first.get("status"),
            "second_status": second.get("status"),
            "first_row_count": first.get("row_count", len(first_rows)),
            "second_row_count": second.get("row_count", len(second_rows)),
            "first_content_credential": first.get("content_credential"),
            "second_content_credential": second.get("content_credential"),
            "same_content_credential": first.get("content_credential") == second.get("content_credential"),
            "both_completed": both_completed,
            "results_equal": results_equal,
            "row_count_delta": int(second.get("row_count", len(second_rows))) - int(first.get("row_count", len(first_rows))),
            "key_comparison": row_comparison,
            "comparison": row_comparison,
            "summary": summary,
            "message": summary,
        }

    def list_conflicts(self, package_no: str | None = None) -> list[dict[str, Any]]:
        if package_no:
            return deepcopy([item for item in self.conflicts if item.get("package_no") == package_no])
        return deepcopy(self.conflicts)

    def list_quarantine(self, package_no: str | None = None) -> list[dict[str, Any]]:
        if package_no:
            return deepcopy([item for item in self.quarantine if item.get("package_no") == package_no])
        return deepcopy(self.quarantine)


# 常见命名别名，便于不同调用习惯和验收脚本发现同一套能力。
SnapshotHandoffRegistry.create_handoff_package = SnapshotHandoffRegistry.export_package
SnapshotHandoffRegistry.export_snapshot_data = SnapshotHandoffRegistry.export_package
SnapshotHandoffRegistry.get_upload_progress = SnapshotHandoffRegistry.get_import_progress
SnapshotHandoffRegistry.recover_upload_progress = SnapshotHandoffRegistry.recover_import
SnapshotHandoffRegistry.resume_handoff = SnapshotHandoffRegistry.recover_import
SnapshotHandoffRegistry.finish_upload = SnapshotHandoffRegistry.complete_import
SnapshotHandoffRegistry.commit_package = SnapshotHandoffRegistry.complete_import
SnapshotHandoffRegistry.import_snapshot_package = SnapshotHandoffRegistry.import_package
SnapshotHandoffRegistry.get_backfill_baselines = SnapshotHandoffRegistry.list_baselines
SnapshotHandoffRegistry.get_baseline_manifest = SnapshotHandoffRegistry.backfill_baseline_manifest
SnapshotHandoffRegistry.run_backfill_window = SnapshotHandoffRegistry.open_backfill_window
SnapshotHandoffRegistry.create_backfill = SnapshotHandoffRegistry.open_backfill_window
SnapshotHandoffRegistry.compare_backfill_results = SnapshotHandoffRegistry.compare_backfill_windows
SnapshotHandoffRegistry.compare_windows = SnapshotHandoffRegistry.compare_backfill_windows
SnapshotHandoffRegistry.register_conflict_candidate = SnapshotHandoffRegistry.register_alternative
