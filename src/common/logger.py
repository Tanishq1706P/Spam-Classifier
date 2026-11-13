"""Logging helpers. Line format follows the plan (section 11)."""
import logging, os, sys
from src.common.config_loader import project_root

_FMT = "%(asctime)s | %(levelname)s | %(message)s"


def get_logger(module):
    lg = logging.getLogger(module)
    if not lg.handlers:
        lg.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(_FMT, "%Y-%m-%d %H:%M:%S"))
        lg.addHandler(sh)
        try:
            d = os.path.join(project_root(), "logs"); os.makedirs(d, exist_ok=True)
            fh = logging.FileHandler(os.path.join(d, "spam.log"))
            fh.setFormatter(logging.Formatter(_FMT, "%Y-%m-%d %H:%M:%S"))
            lg.addHandler(fh)
        except OSError:
            pass
    return lg


def log_error(err, phase="-", where="-"):
    get_logger("spam").error(
        f"{err.code} | {phase} | {where} | {err.message} | cause={err.cause} | hint={err.hint}")
