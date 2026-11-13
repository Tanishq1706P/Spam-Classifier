"""HBase access via Thrift (happybase). Falls back to an in-memory store when HBase is
unreachable and SPAM_ALLOW_MEMORY_STORE=1 (local development / unit tests only)."""
import os, threading, time
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.logger import get_logger

log = get_logger("hbase")
_lock = threading.Lock()


class MemoryStore:
    """Mimics the tiny subset of happybase we use."""
    def __init__(self):
        self.t = {}

    def put(self, table, key, data):
        self.t.setdefault(table, {}).setdefault(key, {}).update(data)

    def get(self, table, key):
        return dict(self.t.get(table, {}).get(key, {}))

    def scan(self, table, prefix="", limit=50):
        rows = sorted(self.t.get(table, {}).items())
        out = [(k, dict(v)) for k, v in rows if k.startswith(prefix)]
        return out[:limit]


_store = None


def _connect():
    global _store
    if _store is not None:
        return _store
    with _lock:
        if _store is not None:
            return _store
        try:
            import happybase
            conn = happybase.Connection(cfg.get("hbase.host"), int(cfg.get("hbase.thrift_port")), timeout=5000)
            conn.open()
            _store = conn
            log.info("connected to HBase Thrift")
        except Exception as e:
            if os.environ.get("SPAM_ALLOW_MEMORY_STORE") == "1":
                log.warning(f"HBase unavailable ({e}); using in-memory store")
                _store = MemoryStore()
            else:
                raise SpamError(ErrorCode.STR_803, cause=repr(e)[:300])
    return _store


def _enc(d):
    return {k.encode(): str(v).encode() for k, v in d.items()}


def _dec(d):
    return {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in d.items()}


def create_tables():
    s = _connect()
    if isinstance(s, MemoryStore):
        return
    fam = {"predictions": {"cf": {}}, "model_runs": {"info": {}, "metrics": {}},
           "phase_log": {"log": {}}, "datasets": {"meta": {}}}
    existing = {t.decode() for t in s.tables()}
    for name, cf in fam.items():
        if name not in existing:
            s.create_table(name, cf)


def put_row(table, key, data):
    global _store
    for i in range(2):
        s = _connect()
        try:
            if isinstance(s, MemoryStore):
                s.put(table, key, data)
            else:
                s.table(table).put(key.encode(), _enc(data))
            return
        except SpamError:
            raise
        except Exception as e:
            _store = None
            if i == 1:
                raise SpamError(ErrorCode.STR_803, cause=repr(e)[:300])


def get_row(table, key):
    global _store
    for i in range(2):
        s = _connect()
        try:
            if isinstance(s, MemoryStore):
                return s.get(table, key)
            return _dec(s.table(table).row(key.encode()))
        except SpamError:
            raise
        except Exception as e:
            _store = None
            if i == 1:
                raise SpamError(ErrorCode.STR_803, cause=repr(e)[:300])


def scan(table, prefix="", limit=50):
    global _store
    for i in range(2):
        s = _connect()
        try:
            if isinstance(s, MemoryStore):
                return [(k, v) for k, v in s.scan(table, prefix, limit)]
            return [(k.decode(), _dec(v)) for k, v in
                    s.table(table).scan(row_prefix=prefix.encode() or None, limit=limit)]
        except SpamError:
            raise
        except Exception as e:
            _store = None
            if i == 1:
                raise SpamError(ErrorCode.STR_803, cause=repr(e)[:300])


# ---- domain helpers --------------------------------------------------------
def prediction_key(email_id):
    """reverse timestamp so newest rows sort first and writes do not hotspot."""
    return f"{9999999999999 - int(time.time() * 1000):013d}#{email_id}"


def save_prediction(email_id, text, label, prob, run_id):
    put_row("predictions", prediction_key(email_id),
            {"cf:text": text[:2000], "cf:label": label, "cf:prob": f"{prob:.4f}",
             "cf:run_id": run_id, "cf:id": email_id})


def recent_predictions(limit=50):
    out = []
    for k, v in scan("predictions", "", limit):
        out.append({"id": v.get("cf:id", k.split("#")[-1]), "text": v.get("cf:text", ""),
                    "label": v.get("cf:label"), "prob": float(v.get("cf:prob", 0) or 0),
                    "run_id": v.get("cf:run_id"), "key": k})
    return out


def phase_sink(entry):
    run = entry.get("run_id") or "adhoc"
    seq = f"{int(time.time() * 1000)}"
    put_row("phase_log", f"{run}#{seq}", {
        "log:phase": entry.get("phase"), "log:status": entry.get("status"),
        "log:code": entry.get("error_code") or "", "log:msg": entry.get("message") or "",
        "log:ts": entry.get("ts")})
