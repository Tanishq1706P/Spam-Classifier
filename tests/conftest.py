import os, sys, tempfile
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ["SPAM_SPARK_MASTER"] = "local[2]"
os.environ["SPAM_ALLOW_MEMORY_STORE"] = "1"
_models = tempfile.mkdtemp(prefix="models_")
os.environ["SPAM_PATHS_HDFS_MODELS"] = _models
SAMPLE = os.path.join(os.path.dirname(__file__), "..", "data", "sample", "sms.csv")


@pytest.fixture(scope="session")
def spark():
    from src.common.spark_session import get_spark
    s = get_spark("local[2]", "tests"); s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def small_csv(tmp_path_factory):
    p = tmp_path_factory.mktemp("d") / "small.csv"
    lines = open(SAMPLE, encoding="utf-8").read().splitlines()
    p.write_text("\n".join(lines[:1801]) + "\n", encoding="utf-8")
    return str(p)
