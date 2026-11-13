"""Regenerate docs/errors.md from src/common/error_codes.py:  python cluster/gen_error_docs.py"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.common.error_codes import CATALOGUE

rows = ["# Error codes", "", "Format: `SPM-<MODULE>-<NNN>`. The code identifies the failing file and phase.", "",
        "| Code | Meaning | Fix |", "|---|---|---|"]
rows += [f"| `{c}` | {m} | {h} |" for c, (m, h) in sorted(CATALOGUE.items())]
rows += ["| `SPM-UI-A01` | API unreachable (browser could not reach the API) | Start uvicorn on the master, check port 8000 |",
         "| `SPM-UI-A02` | Malformed response from the API | Check `logs/spam.log` |", ""]
open(os.path.join(os.path.dirname(__file__), "..", "docs", "errors.md"), "w").write("\n".join(rows))
