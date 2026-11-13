"""P6 SAVE_MODEL: versioned PipelineModels under hdfs /models/vN. Error SPM-MOD-7xx."""
import re
from pyspark.ml import PipelineModel
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import tracked
from src.storage import hdfs_client


def list_versions(spark, base=None):
    base = base or cfg.get("paths.hdfs_models")
    vs = []
    for p in hdfs_client.ls(spark, base):
        m = re.search(r"/v(\d+)$", p)
        if m:
            vs.append(int(m.group(1)))
    return sorted(vs)


@tracked("SAVE_MODEL")
def save_model(spark, model, base=None):
    base = base or cfg.get("paths.hdfs_models")
    ver = (list_versions(spark, base) or [0])[-1] + 1
    path = f"{base.rstrip('/')}/v{ver}"
    try:
        model.write().overwrite().save(path)
    except Exception as e:
        raise SpamError(ErrorCode.MOD_701, cause=repr(e)[:300])
    return ver, path


def load_latest(spark, base=None):
    base = base or cfg.get("paths.hdfs_models")
    vs = list_versions(spark, base)
    if not vs:
        raise SpamError(ErrorCode.MOD_702, cause=f"no versions under {base}")
    path = f"{base.rstrip('/')}/v{vs[-1]}"
    try:
        return PipelineModel.load(path), vs[-1], path
    except Exception as e:
        raise SpamError(ErrorCode.STR_802, cause=repr(e)[:300])
