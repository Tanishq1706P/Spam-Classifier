from fastapi import APIRouter, Query
from src.api import service
from src.common import phase_tracker as pt
from src.common.error_codes import CATALOGUE

router = APIRouter()


@router.get("/status")
def status():
    cur = pt.get_current_phase()
    job = service.job_state()
    return {"phase": cur["phase"], "status": cur["status"], "error_code": cur["error_code"],
            "message": cur["message"], "cause": cur["cause"], "hint": cur["hint"],
            "phases": pt.PHASES, "job_running": job["running"], "nodes": service.cluster_health()}


@router.get("/metrics")
def metrics(run_id: str = Query(None)):
    m = service.last_metrics()
    if run_id and m.get("run_id") != run_id:
        from src.storage import hbase_client as hb
        row = hb.get_row("model_runs", run_id)
        return {"run_id": run_id, "raw": row}
    return m


@router.get("/logs")
def logs(limit: int = Query(100, ge=1, le=500), code: str = Query(None)):
    h = pt.get_history(limit)
    return [x for x in h if not code or x.get("error_code") == code]


@router.get("/errors")
def errors():
    return {k: {"message": v[0], "hint": v[1]} for k, v in CATALOGUE.items()}
