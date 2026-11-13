"""HDFS helpers using the Hadoop FileSystem API through Spark's JVM (works for file:// too)."""
from src.common.error_codes import ErrorCode, SpamError


def _fs(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return jvm, p, p.getFileSystem(spark._jsc.hadoopConfiguration())


def exists(spark, path):
    _, p, fs = _fs(spark, path)
    return fs.exists(p)


def ls(spark, path):
    _, p, fs = _fs(spark, path)
    if not fs.exists(p):
        return []
    return sorted(s.getPath().toString() for s in fs.listStatus(p))


def put(spark, local_path, hdfs_path):
    try:
        jvm, p, fs = _fs(spark, hdfs_path)
        fs.copyFromLocalFile(False, True, jvm.org.apache.hadoop.fs.Path(local_path), p)
    except Exception as e:
        raise SpamError(ErrorCode.ING_203, cause=repr(e)[:300])


def get(spark, hdfs_path, local_path):
    jvm, p, fs = _fs(spark, hdfs_path)
    fs.copyToLocalFile(False, p, jvm.org.apache.hadoop.fs.Path(local_path))
