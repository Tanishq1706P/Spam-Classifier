"""P1 INGEST. Error range SPM-ING-2xx."""
import os
from pyspark.sql import functions as F
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import tracked
from src.storage import hdfs_client


def validate_schema(df):
    cols = {c.lower() for c in df.columns}
    if not {"label", "text"} <= cols:
        raise SpamError(ErrorCode.ING_202, cause=f"found columns={df.columns}")
    return df.select(F.col("label"), F.col("text"))


@tracked("INGEST")
def read_csv_to_hdfs(spark, local_path, hdfs_dir=None):
    """Upload a local CSV (label,text) to HDFS and return the HDFS file path."""
    if not os.path.exists(local_path):
        raise SpamError(ErrorCode.ING_201, cause=f"path={local_path}")
    hdfs_dir = hdfs_dir or cfg.get("paths.hdfs_raw")
    dest = hdfs_dir.rstrip("/") + "/" + os.path.basename(local_path)
    hdfs_client.put(spark, os.path.abspath(local_path), dest)
    df = spark.read.csv(dest, header=True, multiLine=True, escape='"')
    validate_schema(df)
    return dest


@tracked("INGEST")
def read_dataset(spark, path):
    try:
        df = spark.read.csv(path, header=True, multiLine=True, escape='"')
    except Exception as e:
        raise SpamError(ErrorCode.ING_201, cause=repr(e)[:300])
    return validate_schema(df)


def register_dataset(name, path, df):
    """Write dataset metadata to HBase `datasets`."""
    from src.storage import hbase_client as hb
    n = df.count()
    spam = df.filter(F.col("label") == "spam").count()
    hb.put_row("datasets", name, {"meta:name": name, "meta:rows": n,
                                   "meta:path": path, "meta:spam_ratio": round(spam / max(n, 1), 4)})
    return {"name": name, "rows": n, "spam_ratio": spam / max(n, 1)}
