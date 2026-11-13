"""P7 STREAM: Kafka emails-in -> Structured Streaming -> label+prob -> HBase + Kafka results.
Run: spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        src/streaming/stream_predictor.py
Error range SPM-STR-8xx."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repo root, so spark-submit works from any cwd
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F, types as T
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.logger import get_logger
from src.common.phase_tracker import start_phase, end_phase, add_sink
from src.common.spark_session import get_spark
from src.ml import model_registry
from src.preprocessing.cleaner import clean_text
from src.storage import hbase_client as hb

log = get_logger("stream")
SCHEMA = T.StructType([T.StructField("id", T.StringType()), T.StructField("text", T.StringType())])
_state = {"model": None, "run_id": None, "query": None}


def score(model, df):
    """df(id,text) -> df(id,text,label,prob). Shared by the stream and unit tests."""
    x = df.withColumn("raw", F.col("text")).withColumn("text", clean_text(F.col("text")))
    out = model.transform(x)
    return out.select("id", F.col("raw").alias("text"),
                      F.when(F.col("prediction") == 1.0, "spam").otherwise("ham").alias("label"),
                      vector_to_array("probability")[1].alias("prob"))


def process_batch(df, batch_id):
    if df.rdd.isEmpty():
        return
    try:
        parsed = (df.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("j"))
                  .select("j.*").filter(F.col("id").isNotNull()))
        scored = score(_state["model"], parsed).cache()
        rows = scored.collect()              # micro-batches are small; keeps HBase writes on the driver
        for r in rows:
            hb.save_prediction(r["id"], r["text"], r["label"], r["prob"], _state["run_id"])
        (scored.select(F.col("id").alias("key"),
                       F.to_json(F.struct("id", "label", "prob")).alias("value"))
         .write.format("kafka").option("kafka.bootstrap.servers", cfg.get("kafka.bootstrap"))
         .option("topic", cfg.get("kafka.topic_out")).save())
        scored.unpersist()
        log.info(f"batch {batch_id}: {len(rows)} emails scored")
    except SpamError as e:
        end_phase("FAIL", e); raise
    except Exception as e:
        err = SpamError(ErrorCode.STR_801, cause=repr(e)[:300])
        end_phase("FAIL", err); raise err from e


def start_stream(spark=None):
    spark = spark or get_spark()
    model, ver, path = model_registry.load_latest(spark)
    _state.update(model=model, run_id=f"v{ver}")
    start_phase("STREAM")
    try:
        src = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", cfg.get("kafka.bootstrap"))
               .option("subscribe", cfg.get("kafka.topic_in"))
               .option("startingOffsets", "latest").option("failOnDataLoss", "false").load())
        q = (src.writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", cfg.get("paths.hdfs_models").rstrip("/") + "/_checkpoints/stream")
             .trigger(processingTime="1 second").start())
    except Exception as e:
        raise SpamError(ErrorCode.STR_801, cause=repr(e)[:300])
    _state["query"] = q
    return q


def stop_stream():
    if _state["query"] is not None:
        _state["query"].stop()
        end_phase("OK")


if __name__ == "__main__":
    add_sink(hb.phase_sink)
    hb.create_tables()
    start_stream().awaitTermination()
