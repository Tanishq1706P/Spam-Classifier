"""Config loader. Error range SPM-CFG-0xx."""
import os
import yaml
from src.common.error_codes import ErrorCode, SpamError

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_cfg = None


def project_root():
    return _ROOT


def load_config(path=None):
    global _cfg
    path = path or os.environ.get("SPAM_CONFIG", os.path.join(_ROOT, "config", "settings.yaml"))
    if not os.path.exists(path):
        raise SpamError(ErrorCode.CFG_001, "Config file missing", f"path={path}")
    with open(path) as f:
        _cfg = yaml.safe_load(f)
    return _cfg


def get(key, default=None):
    """Dotted lookup, e.g. get('kafka.topic_in'). Env var SPAM_<KEY> overrides."""
    cfg = _cfg or load_config()
    env = os.environ.get("SPAM_" + key.upper().replace(".", "_"))
    if env is not None:
        return env
    cur = cfg
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            if default is not None:
                return default
            raise SpamError(ErrorCode.CFG_002, "Invalid config key", f"key={key}")
        cur = cur[part]
    return cur
