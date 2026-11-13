"""Single place that builds the SparkSession (yarn on the cluster, local[*] for dev)."""
from pyspark.sql import SparkSession
from src.common import config_loader as cfg


def get_spark(master=None, app=None):
    b = (SparkSession.builder
         .master(master or cfg.get("spark.master"))
         .appName(app or cfg.get("spark.app_name"))
         .config("spark.executor.memory", str(cfg.get("spark.executor_memory", "1g")))
         .config("spark.sql.shuffle.partitions", "8"))
    return b.getOrCreate()
