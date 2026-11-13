"""P5 EVALUATE. Error SPM-EVL-6xx."""
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import functions as F
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import tracked


def confusion_matrix(pred_df):
    r = {(int(x["label"]), int(x["prediction"])): x["count"]
         for x in pred_df.groupBy("label", "prediction").count().collect()}
    return {"tn": r.get((0, 0), 0), "fp": r.get((0, 1), 0), "fn": r.get((1, 0), 0), "tp": r.get((1, 1), 0)}


@tracked("EVALUATE")
def evaluate(model, test_df, enforce_threshold=False):
    if test_df.rdd.isEmpty():
        raise SpamError(ErrorCode.EVL_602)
    pred = model.transform(test_df)
    mc = lambda m: MulticlassClassificationEvaluator(labelCol="label", metricName=m)
    cm = confusion_matrix(pred)
    tp, fp, fn, tn = cm["tp"], cm["fp"], cm["fn"], cm["tn"]
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    m = {"accuracy": mc("accuracy").evaluate(pred), "precision": prec, "recall": rec, "f1": f1,
         "auc": BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability", metricName="areaUnderROC").evaluate(pred),
         "fpr": fp / (fp + tn) if fp + tn else 0.0, "confusion": cm, "test_rows": tp + fp + fn + tn}
    m = {k: (round(v, 4) if isinstance(v, float) else v) for k, v in m.items()}
    if enforce_threshold and m["accuracy"] < float(cfg.get("ml.min_accuracy", 0.95)):
        raise SpamError(ErrorCode.EVL_601, cause=f"accuracy={m['accuracy']}")
    return m


def save_metrics(run_id, algo, params, model_path, metrics):
    import json
    from src.storage import hbase_client as hb
    hb.put_row("model_runs", run_id, {
        "info:algo": algo, "info:params": json.dumps(params or {}), "info:path": model_path,
        "metrics:acc": metrics["accuracy"], "metrics:prec": metrics["precision"],
        "metrics:rec": metrics["recall"], "metrics:f1": metrics["f1"], "metrics:auc": metrics["auc"],
        "metrics:fpr": metrics["fpr"], "metrics:confusion": json.dumps(metrics["confusion"])})
