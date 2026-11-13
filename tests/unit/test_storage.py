from src.storage import hbase_client as hb


def test_prediction_roundtrip_newest_first():
    import time
    hb.save_prediction("a", "first", "ham", 0.1, "v1"); time.sleep(0.01)
    hb.save_prediction("b", "second", "spam", 0.9, "v1")
    rows = hb.recent_predictions(10)
    assert rows[0]["id"] == "b" and rows[0]["label"] == "spam" and rows[1]["id"] == "a"


def test_phase_sink_writes_row():
    hb.phase_sink({"phase": "TRAIN", "status": "FAIL", "error_code": "SPM-TRN-501", "message": "m", "ts": "t", "run_id": "r1"})
    assert any(k.startswith("r1#") for k, _ in hb.scan("phase_log", "r1#"))
