"""P4 TRAIN. ALGO_REGISTRY makes new classifiers a one-line addition. Error SPM-TRN-5xx."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repo root, so spark-submit works from any cwd
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import tracked
from src.ml.features import build_pipeline

def _logreg(p):
    kw = dict(labelCol="label", maxIter=int(p.get("maxIter", 50)), regParam=float(p.get("regParam", 0.01)))
    if p.get("weighted", True):
        kw["weightCol"] = "weight"      # class-balancing weights (see _with_weights)
    return LogisticRegression(**kw)


ALGO_REGISTRY = {
    "logreg": _logreg,
    "nb": lambda p: NaiveBayes(labelCol="label", smoothing=float(p.get("smoothing", 1.0))),
}


def split_data(df, ratio=None, seed=42):
    r = float(ratio or cfg.get("ml.train_ratio", 0.8))
    train, test = df.randomSplit([r, 1 - r], seed)
    if test.rdd.isEmpty():
        raise SpamError(ErrorCode.EVL_602)
    return train, test


def _with_weights(df):
    """Add a per-row `weight` column so the minority (spam) class counts as much as ham."""
    from pyspark.sql import functions as F
    n = df.count(); pos = df.filter(F.col("label") == 1.0).count(); neg = n - pos
    if pos == 0 or neg == 0:
        raise SpamError(ErrorCode.TRN_502, cause=f"pos={pos} neg={neg}")
    return df.withColumn("weight", F.when(F.col("label") == 1.0, n / (2.0 * pos)).otherwise(n / (2.0 * neg)))


def _check_balance(train):
    rows = {r["label"]: r["count"] for r in train.groupBy("label").count().collect()}
    if len(rows) < 2:
        raise SpamError(ErrorCode.TRN_502, cause=f"only one class present: {rows}")


@tracked("TRAIN")
def train(df, algo=None, params=None):
    algo = algo or cfg.get("ml.default_algo", "logreg")
    if algo not in ALGO_REGISTRY:
        raise SpamError(ErrorCode.API_901, cause=f"unknown algo '{algo}', choose {list(ALGO_REGISTRY)}")
    _check_balance(df)
    params = params or {}
    clf = ALGO_REGISTRY[algo](params)
    if algo == "logreg" and params.get("weighted", True):
        df = _with_weights(df)
    return build_pipeline(clf).fit(df)


@tracked("TRAIN")
def tune_cv(df, algo="logreg", folds=None):
    """3-fold CrossValidator over regParam (logreg) / smoothing (nb)."""
    clf = ALGO_REGISTRY[algo]({})
    if algo == "logreg":
        df = _with_weights(df)
    pipe = build_pipeline(clf)
    grid = ParamGridBuilder()
    grid = grid.addGrid(clf.regParam, [0.001, 0.01, 0.1]) if algo == "logreg" else grid.addGrid(clf.smoothing, [0.5, 1.0])
    cv = CrossValidator(estimator=pipe, estimatorParamMaps=grid.build(),
                        evaluator=BinaryClassificationEvaluator(labelCol="label"),
                        numFolds=int(folds or cfg.get("ml.cv_folds", 3)), seed=42)
    return cv.fit(df).bestModel


if __name__ == "__main__":      # spark-submit src/ml/trainer.py  (delegates to the full batch job)
    from src.ml.train_job import main
    main()
