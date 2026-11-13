"""FastAPI app. Run: uvicorn src.api.main:app --host 0.0.0.0 --port 8000
Routes are served under /api/v1 and also /api (alias). The UI in ui/ is served at /."""
import os
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from src.common import config_loader as cfg
from src.common.error_codes import ErrorCode, SpamError
from src.common.phase_tracker import get_current_phase
from src.api import routes_train, routes_predict, routes_status

app = FastAPI(title="Spam Classifier API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:8000", "http://127.0.0.1:8000"],
                   allow_methods=["GET", "POST"], allow_headers=["*"])

for prefix in ("/api/v1", "/api"):
    for r in (routes_train.router, routes_predict.router, routes_status.router):
        app.include_router(r, prefix=prefix)


@app.exception_handler(SpamError)
async def spam_error_handler(request: Request, exc: SpamError):
    code = 404 if exc.code == ErrorCode.MOD_702.value else 409 if exc.code == ErrorCode.API_902.value else \
        400 if exc.code.startswith("SPM-API") else 500
    return JSONResponse(status_code=code, content=exc.to_dict(get_current_phase().get("phase") or ""))


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    e = SpamError(ErrorCode.API_901, cause=str(exc.errors())[:300])
    return JSONResponse(status_code=400, content=e.to_dict(""))


_ui = os.path.join(cfg.project_root(), "ui")
if os.path.isdir(_ui):
    app.mount("/", StaticFiles(directory=_ui, html=True), name="ui")
