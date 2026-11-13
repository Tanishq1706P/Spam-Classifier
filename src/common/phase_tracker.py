"""Phase tracking P1..P7: START/OK/FAIL + duration. The API and UI read the latest entry."""
import functools, threading, time
from src.common.error_codes import ErrorCode as E, SpamError
from src.common.logger import get_logger, log_error

PHASES = ["INGEST", "CLEAN", "FEATURES", "TRAIN", "EVALUATE", "SAVE_MODEL", "STREAM"]
_lock = threading.Lock()
_current = {"phase": None, "status": "IDLE", "error_code": None, "message": "", "cause": "", "hint": ""}
_history = []          # in-memory log (also mirrored to HBase phase_log via sinks)
_sinks = []
_log = get_logger("phase")


def add_sink(fn):
    _sinks.append(fn)


def _emit(entry):
    entry = dict(entry, ts=time.strftime("%Y-%m-%d %H:%M:%S"))
    with _lock:
        _current.update(entry)
        _history.append(dict(entry))
        del _history[:-500]
    for s in _sinks:
        try:
            s(dict(entry))
        except Exception as ex:  # sinks must never break a job
            _log.warning(f"phase sink failed: {ex}")


def start_phase(name):
    _log.info(f"{name} START")
    _emit({"phase": name, "status": "START", "error_code": None, "message": "", "cause": "", "hint": ""})


def end_phase(status="OK", err=None, duration=None):
    name = _current["phase"]
    e = {"phase": name, "status": status, "duration": duration}
    if err:
        e.update(error_code=err.code, message=err.message, cause=err.cause, hint=err.hint)
    _log.info(f"{name} {status} duration={duration}")
    _emit(e)


def get_current_phase():
    with _lock:
        return dict(_current)


def get_history(limit=100):
    with _lock:
        if _history:
            return list(_history[-limit:])
    try:
        from src.storage import hbase_client as hb
        rows = hb.scan("phase_log", "", limit)
        out = [{"ts": v.get("log:ts", ""), "phase": v.get("log:phase", ""),
                "status": v.get("log:status", ""), "error_code": v.get("log:code") or None,
                "message": v.get("log:msg", ""), "cause": ""} for _, v in rows]
        out.sort(key=lambda x: x["ts"])
        return out[-limit:]
    except Exception:
        return []


def reset():
    with _lock:
        _current.update(phase=None, status="IDLE", error_code=None, message="", cause="", hint="")
        _history.clear()


def _classify(exc, phase):
    """Map a low-level exception to a coded error."""
    s = str(exc).lower()
    if "outofmemory" in s or "java heap" in s:
        return E.TRN_501
    if "path does not exist" in s or isinstance(exc, FileNotFoundError):
        return E.ING_201
    return {"INGEST": E.ING_203, "CLEAN": E.PRE_301, "FEATURES": E.FEA_401, "TRAIN": E.TRN_503,
            "EVALUATE": E.EVL_602, "SAVE_MODEL": E.MOD_701, "STREAM": E.STR_801}.get(phase, E.API_903)


def tracked(phase):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            start_phase(phase); t0 = time.time()
            try:
                out = fn(*a, **kw)
            except SpamError as e:
                end_phase("FAIL", e, round(time.time() - t0, 2)); log_error(e, phase, fn.__name__); raise
            except Exception as e:
                se = SpamError(_classify(e, phase), cause=repr(e)[:300])
                end_phase("FAIL", se, round(time.time() - t0, 2)); log_error(se, phase, fn.__name__)
                raise se from e
            end_phase("OK", duration=round(time.time() - t0, 2))
            return out
        return wrapper
    return deco
