import os, pytest
from src.common.error_codes import ErrorCode, SpamError, CATALOGUE, describe
from src.common import phase_tracker as pt, config_loader as cfg


def test_every_code_has_catalogue_entry():
    for c in ErrorCode:
        assert c.value in CATALOGUE


def test_every_code_documented():            # plan §11: CI fails if a code is undocumented
    doc = open(os.path.join(os.path.dirname(__file__), "..", "..", "docs", "errors.md")).read()
    for c in ErrorCode:
        assert c.value in doc


def test_spam_error_dict():
    d = SpamError(ErrorCode.TRN_501, cause="Executor OOM").to_dict("TRAIN")
    assert d["error_code"] == "SPM-TRN-501" and d["phase"] == "TRAIN" and d["cause"] == "Executor OOM" and d["hint"]


def test_phase_tracker_ok_and_fail():
    pt.reset()

    @pt.tracked("CLEAN")
    def ok(): return 5

    @pt.tracked("TRAIN")
    def bad(): raise SpamError(ErrorCode.TRN_501, cause="oom")

    assert ok() == 5 and pt.get_current_phase()["status"] == "OK"
    with pytest.raises(SpamError):
        bad()
    cur = pt.get_current_phase()
    assert cur["status"] == "FAIL" and cur["error_code"] == "SPM-TRN-501" and cur["phase"] == "TRAIN"
    assert [h["status"] for h in pt.get_history()] == ["START", "OK", "START", "FAIL"]


def test_tracked_wraps_foreign_exceptions():
    pt.reset()

    @pt.tracked("INGEST")
    def boom(): raise FileNotFoundError("nope")
    with pytest.raises(SpamError) as e:
        boom()
    assert e.value.code == "SPM-ING-201"


def test_config_missing_key_and_env_override(monkeypatch):
    with pytest.raises(SpamError) as e:
        cfg.get("no.such.key")
    assert e.value.code == "SPM-CFG-002"
    monkeypatch.setenv("SPAM_KAFKA_TOPIC_IN", "x")
    assert cfg.get("kafka.topic_in") == "x"


def test_config_missing_file():
    with pytest.raises(SpamError) as e:
        cfg.load_config("/nonexistent.yaml")
    assert e.value.code == "SPM-CFG-001"
