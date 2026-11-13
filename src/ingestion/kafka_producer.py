"""Kafka producer for the `emails-in` topic. Error SPM-ING-204."""
import json, time, uuid
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError

_producer = None


def _get():
    global _producer
    if _producer is None:
        try:
            from kafka import KafkaProducer
            _producer = KafkaProducer(bootstrap_servers=cfg.get("kafka.bootstrap"),
                                      value_serializer=lambda v: json.dumps(v).encode(),
                                      request_timeout_ms=5000)
        except Exception as e:
            raise SpamError(ErrorCode.ING_204, cause=repr(e)[:300])
    return _producer


def send_email(text, email_id=None):
    email_id = email_id or uuid.uuid4().hex[:12]
    try:
        p = _get()
        p.send(cfg.get("kafka.topic_in"), {"id": email_id, "text": text}).get(timeout=5)
    except SpamError:
        raise
    except Exception as e:
        raise SpamError(ErrorCode.ING_204, cause=repr(e)[:300])
    return email_id


def replay_dataset(path, rate=5, limit=None):
    """Replay a CSV (label,text) into Kafka at `rate` messages/second."""
    import csv
    with open(path, newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            if limit and i >= limit:
                break
            send_email(row["text"])
            time.sleep(1.0 / rate)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=cfg.get("paths.local_sample"))
    ap.add_argument("--rate", type=float, default=5)
    ap.add_argument("--limit", type=int, default=100)
    a = ap.parse_args()
    replay_dataset(a.file, a.rate, a.limit)
