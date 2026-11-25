"""快照交接包 HTTP API：导出、分段导入、冲突登记、基准清单和回填窗口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from app.store import store

router = APIRouter(prefix="/api/snapshot-handoff", tags=["快照交接"])


async def _json(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:  # noqa: BLE001 - 空请求按空参数处理，由领域层给出业务错误
        payload = {}
    return payload if isinstance(payload, dict) else {"payload": payload}


@router.post("/packages/export")
@router.post("/export")
async def export_package(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.export_package(**payload)


@router.get("/packages/{package_no}/export")
async def export_registered_package(package_no: str) -> Response:
    baseline = store.handoff.get_baseline(package_no)
    if baseline is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"{package_no} 没有已核验基准"})
    package = baseline.get("package") or baseline
    return JSONResponse(
        package,
        headers={"Content-Disposition": f'attachment; filename="{package_no}-{baseline.get("snapshot_version", "v1")}.json"'},
    )


@router.post("/packages/verify")
async def verify_package(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    package = payload.get("package", payload)
    return store.handoff.verify_package(
        package,
        expected_source_table=payload.get("expected_source_table"),
        expected_version=payload.get("expected_version"),
        expected_package_no=payload.get("expected_package_no"),
    )


@router.post("/imports")
@router.post("/uploads")
async def start_import(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.start_import(**payload)


@router.post("/packages/import")
@router.post("/import")
async def import_package(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    package = payload.get("package", payload)
    options = {k: v for k, v in payload.items() if k not in {"package"}}
    if isinstance(package, dict) and options:
        merged = {**package, **options}
        return store.handoff.import_package(merged)
    return store.handoff.import_package(package, **options)


@router.get("/imports/{upload_id}")
@router.get("/uploads/{upload_id}")
async def import_progress(upload_id: str) -> dict[str, Any]:
    try:
        return store.handoff.get_import_progress(upload_id)
    except KeyError as exc:
        return JSONResponse(status_code=404, content={"ok": False, "message": str(exc)})  # type: ignore[return-value]


@router.post("/imports/{upload_id}/interrupt")
async def interrupt_import(upload_id: str, request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.interrupt_import(upload_id, payload.get("reason", "client_disconnected"))


@router.post("/imports/{upload_id}/resume")
async def resume_import(upload_id: str) -> dict[str, Any]:
    return store.handoff.recover_import(upload_id)


@router.post("/imports/{upload_id}/complete")
@router.post("/imports/{upload_id}/finish")
async def complete_import(upload_id: str, request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.complete_import(upload_id, **payload)


@router.put("/imports/{upload_id}/segments/{segment_ref}")
@router.post("/imports/{upload_id}/segments/{segment_ref}")
@router.put("/uploads/{upload_id}/parts/{segment_ref}")
@router.post("/uploads/{upload_id}/parts/{segment_ref}")
async def upload_segment(upload_id: str, segment_ref: str, request: Request) -> dict[str, Any]:
    payload = await _json(request)
    try:
        ref: Any = int(segment_ref)
    except ValueError:
        ref = segment_ref
    return store.handoff.upload_segment(upload_id, ref, payload)


@router.post("/candidates/{candidate_id}/complete")
async def complete_candidate(candidate_id: str, request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.complete_import(candidate_id, **payload)


@router.post("/candidates/{candidate_id}/register-alternative")
@router.post("/candidates/{candidate_id}/separate-registration")
async def register_alternative(candidate_id: str, request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.register_alternative(candidate_id, **payload)


@router.get("/baselines")
@router.get("/backfill-baselines")
async def baselines(source_table: str | None = None) -> dict[str, Any]:
    items = store.handoff.list_baselines(source_table)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/conflicts")
async def conflicts(package_no: str | None = None) -> dict[str, Any]:
    items = store.handoff.list_conflicts(package_no)
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/quarantine")
async def quarantine(package_no: str | None = None) -> dict[str, Any]:
    items = store.handoff.list_quarantine(package_no)
    return {"ok": True, "items": items, "total": len(items)}


@router.post("/backfill-windows")
@router.post("/backfill")
async def open_window(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    return store.handoff.open_backfill_window(**payload)


@router.get("/backfill-windows")
async def list_windows(package_no: str | None = None) -> dict[str, Any]:
    windows = list(store.handoff.windows.values())
    if package_no:
        windows = [item for item in windows if item.get("package_no") == package_no]
    # 列表不重复携带每行数据；详情接口仍提供完整结果。
    items = [{k: v for k, v in item.items() if k not in {"rows", "backfilled_rows", "actual_data", "key_results", "comparison"}}
             for item in windows]
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/backfill-windows/{window_id}")
async def get_window(window_id: str) -> JSONResponse:
    window = store.handoff.get_backfill_window(window_id)
    if window is None:
        return JSONResponse(status_code=404, content={"ok": False, "message": f"找不到窗口 {window_id}"})
    return JSONResponse(window)


@router.post("/backfill-windows/compare")
async def compare_windows(request: Request) -> dict[str, Any]:
    payload = await _json(request)
    first = payload.get("first_window_id") or payload.get("first")
    second = payload.get("second_window_id") or payload.get("second")
    return store.handoff.compare_backfill_windows(first, second)


@router.post("/reset")
async def reset_handoff_state() -> dict[str, Any]:
    store.reset_runtime_state()
    return {"ok": True, "message": "交接包运行时状态已重置，种子业务清单保留"}
