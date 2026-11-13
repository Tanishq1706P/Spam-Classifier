"""End-to-end batch job (P1-P6): spark-submit --master yarn src/ml/train_job.py --algo logreg"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repo root, so spark-submit works from any cwd
import argparse, json, time
from src.common import config_loader as cfg
from src.common.phase_tracker import add_sink
from src.common.spark_session import get_spark
from src.ingestion import loader
from src.preprocessing import cleaner
from src.ml import trainer, evaluator, model_registry


def run_training(spark, data_path, algo="logreg", params=None, tune=False, balance=False, persist=True):
    run_id = time.strftime("run%Y%m%d%H%M%S")
    raw = loader.read_dataset(spark, data_path)
    clean = cleaner.clean_dataframe(raw, balance=balance).cache()
    tr, te = trainer.split_data(clean)
    model = trainer.tune_cv(tr, algo) if tune else trainer.train(tr, algo, params)
    metrics = evaluator.evaluate(model, te)
    ver, path = model_registry.save_model(spark, model)
    if persist:
        evaluator.save_metrics(run_id, algo, params, path, metrics)
    return {"run_id": run_id, "version": ver, "model_path": path, "metrics": metrics}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=cfg.get("paths.hdfs_raw") + "/sms.csv")
    ap.add_argument("--algo", default=cfg.get("ml.default_algo"))
    ap.add_argument("--tune", action="store_true")
    ap.add_argument("--master", default=None)
    a = ap.parse_args()
    from src.storage import hbase_client as hb
    add_sink(hb.phase_sink)
    spark = get_spark(a.master)
    print(json.dumps(run_training(spark, a.data, a.algo, tune=a.tune), indent=2))


if __name__ == "__main__":
    main()
