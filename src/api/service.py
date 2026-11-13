"""Shared state for the API: Spark session, cached model, job runner, cluster health."""
import socket, threading, time, yaml, os
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.logger import get_logger
from src.common import phase_tracker as pt

log = get_logger("api")
_lock = threading.Lock()
_spark = None
_model = {"m": None, "version": None, "path": None}
_job = {"running": False, "run_id": None, "result": None}
_last_metrics = {}


def spark():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = SparkSession.builder.master("local[1]").appName("spam-api").getOrCreate()
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


def model():
    if _model["m"] is None:
        from src.ml import model_registry
        m, v, p = model_registry.load_latest(spark())
        _model.update(m=m, version=v, path=p)
    return _model


def invalidate_model():
    _model.update(m=None, version=None, path=None)


def predict_one(text):
    from pyspark.sql import Row
    from src.streaming.stream_predictor import score
    m = model()
    df = spark().createDataFrame([Row(id="adhoc", text=text)])
    r = score(m["m"], df).collect()[0]
    return {"label": r["label"], "prob": round(float(r["prob"]), 4), "run_id": f"v{m['version']}"}


def start_training(dataset, algo, tune=False):
    with _lock:
        if _job["running"]:
            raise SpamError(ErrorCode.API_902, cause=f"job {_job['run_id']} is running")
        _job.update(running=True, result=None, run_id=time.strftime("run%Y%m%d%H%M%S"))
    rid = _job["run_id"]

    def work():
        try:
            from src.ml.train_job import run_training
            res = run_training(spark(), dataset, algo, tune=tune)
            _job["result"] = res
            _last_metrics.update(res["metrics"], run_id=res["run_id"], version=res["version"], algo=algo)
            invalidate_model()
        except SpamError as e:
            _job["result"] = {"error": e.to_dict(pt.get_current_phase().get("phase") or "TRAIN")}
        except Exception as e:  # never let the thread die silently
            _job["result"] = {"error": SpamError(ErrorCode.API_903, cause=repr(e)[:300]).to_dict("TRAIN")}
        finally:
            _job["running"] = False

    threading.Thread(target=work, daemon=True).start()
    return rid


def job_state():
    return dict(_job)


def last_metrics():
    if _last_metrics:
        return dict(_last_metrics)
    try:
        import json
        from src.storage import hbase_client as hb
        rows = hb.scan("model_runs", "", 20)
        if rows:
            rid, d = sorted(rows)[-1]
            path = d.get("info:path", "/models/v4")
            ver = int(path.split("/v")[-1]) if "/v" in path else 1
            _last_metrics.update(
                run_id=rid, version=ver, algo=d.get("info:algo", "logreg"),
                accuracy=float(d.get("metrics:acc", 0)), precision=float(d.get("metrics:prec", 0)),
                recall=float(d.get("metrics:rec", 0)), f1=float(d.get("metrics:f1", 0)),
                auc=float(d.get("metrics:auc", 0)), fpr=float(d.get("metrics:fpr", 0)),
                confusion=json.loads(d.get("metrics:confusion", "{}"))
            )
    except Exception:
        pass
    return dict(_last_metrics)


# ---- cluster health: TCP probes of well-known daemon ports --------------------
def _probe(host, port, t=0.8):
    try:
        with socket.create_connection((host, port), timeout=t):
            return True
    except OSError:
        return False


def cluster_health():
    path = os.path.join(cfg.project_root(), "config", "cluster.yaml")
    nodes = yaml.safe_load(open(path))["nodes"]
    out = []
    for name, n in nodes.items():
        h = n["host"]
        if name == "master":
            svc = {"NameNode": 9870, "ResourceManager": 8088, "Kafka": 9092, "HBaseMaster": 16010}
        else:
            svc = {"DataNode": 9864, "NodeManager": 8042, "RegionServer": 16030}
        s = {k: _probe(h, p) for k, p in svc.items()}
        out.append({"node": name, "host": h, "role": "master" if name == "master" else "worker",
                    "services": s, "up": all(s.values()), "partial": any(s.values()) and not all(s.values())})
    return out
