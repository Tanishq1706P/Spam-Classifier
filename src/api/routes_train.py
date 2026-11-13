from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel
from src.api import service
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError

router = APIRouter()


class TrainIn(BaseModel):
    dataset: Optional[str] = None   # HDFS (or local in dev) path to a CSV with label,text
    algo: str = "logreg"
    tune: bool = False


@router.post("/train")
def train(body: TrainIn):
    from src.ml.trainer import ALGO_REGISTRY
    if body.algo not in ALGO_REGISTRY:
        raise SpamError(ErrorCode.API_901, cause=f"algo must be one of {list(ALGO_REGISTRY)}")
    dataset = body.dataset or cfg.get("paths.hdfs_raw") + "/sms.csv"
    rid = service.start_training(dataset, body.algo, body.tune)
    return {"run_id": rid, "phase": "INGEST"}


@router.get("/train/result")
def train_result():
    return service.job_state()
