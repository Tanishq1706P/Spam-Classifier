import uuid
from fastapi import APIRouter, Query
from pydantic import BaseModel
from src.api import service
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.storage import hbase_client as hb

router = APIRouter()


class PredictIn(BaseModel):
    text: str


def _validate(text):
    text = (text or "").strip()
    if not text:
        raise SpamError(ErrorCode.API_901, cause="text is empty")
    if len(text) > int(cfg.get("api.max_text_len", 20000)):
        raise SpamError(ErrorCode.API_901, cause="text too long")
    return text


@router.post("/predict")
def predict(body: PredictIn):
    text = _validate(body.text)
    res = service.predict_one(text)
    hb.save_prediction(uuid.uuid4().hex[:12], text, res["label"], res["prob"], res["run_id"])
    return res


@router.post("/send")
def send_to_stream(body: PredictIn):
    """Push an email to Kafka `emails-in`; the streaming job classifies it and stores it in HBase."""
    from src.ingestion import kafka_producer
    return {"id": kafka_producer.send_email(_validate(body.text)), "queued": True}


@router.get("/predictions")
def predictions(limit: int = Query(50, ge=1, le=500)):
    return hb.recent_predictions(limit)
