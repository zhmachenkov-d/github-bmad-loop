"""FastAPI health stub — no Slack/GitHub/panel/signal routes."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import Response

app = FastAPI(title="bmad-gateway", docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
def health() -> Response:
    return Response(status_code=200)
