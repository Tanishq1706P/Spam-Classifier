"""P3 FEATURES: Tokenizer -> StopWordsRemover -> HashingTF -> IDF. Error SPM-FEA-4xx."""
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError


def build_feature_stages(num_features=None):
    n = int(num_features or cfg.get("spark.num_features", 2 ** 18))
    return [Tokenizer(inputCol="text", outputCol="w"),
            StopWordsRemover(inputCol="w", outputCol="f"),
            HashingTF(inputCol="f", outputCol="tf", numFeatures=n),
            IDF(inputCol="tf", outputCol="features")]


def build_pipeline(classifier, num_features=None):
    if classifier is None:
        raise SpamError(ErrorCode.FEA_401, cause="classifier is None")
    return Pipeline(stages=build_feature_stages(num_features) + [classifier])
