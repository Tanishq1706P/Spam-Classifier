import pytest
from pyspark.sql import Row
from src.common.error_codes import SpamError
from src.ingestion import loader
from src.preprocessing import cleaner
from src.ml import trainer, evaluator, model_registry
from src.streaming.stream_predictor import score


def test_clean_text_edge_cases(spark):
    rows = [("<b>FREE</b> prize!!! visit http://x.com NOW 12345",), ("",), (None,), ("héllo wörld",)]
    df = spark.createDataFrame(rows, ["t"])
    out = [r[0] for r in df.select(cleaner.clean_text(df.t)).collect()]
    assert out[0] == "free prize visit url now"
    assert out[1] == "" and out[2] == ""
    assert "h" in out[3]  # non-English chars removed, no crash


def test_schema_validation(spark):
    bad = spark.createDataFrame([("a", "b")], ["x", "y"])
    with pytest.raises(SpamError) as e:
        loader.validate_schema(bad)
    assert e.value.code == "SPM-ING-202"


def test_missing_file(spark):
    with pytest.raises(SpamError) as e:
        loader.read_csv_to_hdfs(spark, "/no/such.csv")
    assert e.value.code == "SPM-ING-201"


def test_empty_after_cleaning(spark):
    df = spark.createDataFrame([("spam", "123 456"), ("ham", "!!!")], ["label", "text"])
    with pytest.raises(SpamError) as e:
        cleaner.clean_dataframe(df)
    assert e.value.code == "SPM-PRE-301"


def test_missing_label_and_duplicates(spark):
    df = spark.createDataFrame([("spam", "win cash"), ("spam", "win cash"), (None, "hello there"), ("ham", "see you soon")], ["label", "text"])
    out = cleaner.clean_dataframe(df)
    assert out.count() == 2


def test_single_class_rejected(spark):
    df = spark.createDataFrame([(0.0, "hello there friend"), (0.0, "see you soon")], ["label", "text"])
    with pytest.raises(SpamError) as e:
        trainer.train(df)
    assert e.value.code == "SPM-TRN-502"


def test_unknown_algo(spark):
    df = spark.createDataFrame([(0.0, "a"), (1.0, "b")], ["label", "text"])
    with pytest.raises(SpamError) as e:
        trainer.train(df, "svm_xyz")
    assert e.value.code == "SPM-API-901"


@pytest.fixture(scope="module")
def trained(spark, small_csv):
    df = cleaner.clean_dataframe(loader.read_dataset(spark, small_csv)).cache()
    tr, te = trainer.split_data(df)
    return trainer.train(tr, "logreg"), te


def test_metrics_quality(trained):
    model, te = trained
    m = evaluator.evaluate(model, te)
    assert m["accuracy"] >= 0.94 and m["fpr"] < 0.02 and 0 <= m["auc"] <= 1
    assert sum(m["confusion"].values()) == m["test_rows"]


def test_threshold_failure(trained):
    model, te = trained
    import os
    os.environ["SPAM_ML_MIN_ACCURACY"] = "1.01"
    try:
        with pytest.raises(SpamError) as e:
            evaluator.evaluate(model, te, enforce_threshold=True)
        assert e.value.code == "SPM-EVL-601"
    finally:
        del os.environ["SPAM_ML_MIN_ACCURACY"]


def test_registry_versions_and_missing(spark, trained, tmp_path):
    model, _ = trained
    base = str(tmp_path / "m")
    with pytest.raises(SpamError) as e:
        model_registry.load_latest(spark, base)
    assert e.value.code == "SPM-MOD-702"
    assert model_registry.save_model(spark, model, base)[0] == 1
    assert model_registry.save_model(spark, model, base)[0] == 2
    _, ver, _ = model_registry.load_latest(spark, base)
    assert ver == 2


def test_score_edge_cases(spark, trained):
    model, _ = trained
    long_text = "free prize " * 5000
    rows = [Row(id="1", text="WINNER!! Claim your free prize now, call 09061701461"),
            Row(id="2", text="ok see you at lunch tomorrow"), Row(id="3", text=""),
            Row(id="4", text="<html><body></body></html>"), Row(id="5", text=long_text), Row(id="6", text="これは日本語です")]
    out = {r["id"]: r for r in score(model, spark.createDataFrame(rows)).collect()}
    assert len(out) == 6 and out["1"]["label"] == "spam" and out["2"]["label"] == "ham"
    assert all(0 <= r["prob"] <= 1 for r in out.values())
