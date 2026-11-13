"""API tests in local mode (in-memory HBase). On the real cluster run with SPAM_ALLOW_MEMORY_STORE unset."""
import time
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client(spark, small_csv):
    from src.api.main import app
    from src.api import service
    service._spark = spark
    c = TestClient(app)
    r = c.post("/api/v1/train", json={"dataset": small_csv, "algo": "logreg"})
    assert r.status_code == 200
    for _ in range(300):
        j = c.get("/api/v1/train/result").json()
        if not j["running"]:
            break
        time.sleep(1)
    assert j["result"] and "error" not in j["result"], j
    return c


def test_status_shape(client):
    s = client.get("/api/v1/status").json()
    assert {"phase", "status", "error_code", "cause", "hint", "nodes", "phases"} <= s.keys()
    assert len(s["nodes"]) == 3 and s["phase"] == "SAVE_MODEL" and s["status"] == "OK"


def test_predict_and_persist(client):
    r = client.post("/api/v1/predict", json={"text": "URGENT! You have won a free prize, call now"}).json()
    assert r["label"] == "spam" and r["run_id"] == "v1"
    assert client.get("/api/predictions?limit=5").json()[0]["label"] == "spam"   # /api alias works


def test_metrics_endpoint(client):
    m = client.get("/api/v1/metrics").json()
    assert m["accuracy"] > 0.9 and "confusion" in m


def test_validation_error_uses_error_schema(client):
    r = client.post("/api/v1/predict", json={"text": "   "})
    assert r.status_code == 400 and r.json()["error_code"] == "SPM-API-901"
    r = client.post("/api/v1/predict", json={"nope": 1})
    assert r.status_code == 400 and r.json()["error_code"] == "SPM-API-901"
    r = client.post("/api/v1/train", json={"algo": "xyz"})
    assert r.json()["error_code"] == "SPM-API-901"


def test_kafka_down_returns_coded_error(client, monkeypatch):
    monkeypatch.setenv("SPAM_KAFKA_BOOTSTRAP", "127.0.0.1:1")
    r = client.post("/api/v1/send", json={"text": "hello"})
    assert r.status_code == 500 and r.json()["error_code"] == "SPM-ING-204"


def test_bad_dataset_triggers_failed_phase_visible_in_status(client):
    from src.api import service
    client.post("/api/v1/train", json={"dataset": "/does/not/exist.csv"})
    for _ in range(60):
        if not service.job_state()["running"]:
            break
        time.sleep(0.5)
    s = client.get("/api/v1/status").json()
    assert s["status"] == "FAIL" and s["error_code"].startswith("SPM-ING") and s["cause"] and s["hint"]


def test_ui_served(client):
    assert "Spam Sorter" in client.get("/").text
