"""Gateway health stub: GET /health → 200; no ingress routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from apps.gateway import __main__ as gateway_main
from apps.gateway.app import app, health


def test_health_returns_200() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.content == b""


def test_health_handler_status_directly() -> None:
    """Kill status_code mutants without going through routing."""
    assert health().status_code == 200


def test_unknown_path_is_not_ingress() -> None:
    client = TestClient(app)
    response = client.post("/slack/events", json={})
    assert response.status_code == 404


def test_main_runs_uvicorn_with_defaults(monkeypatch) -> None:
    """Kill __main__ host/port/app-path mutants (no live server)."""
    monkeypatch.delenv("GATEWAY_HOST", raising=False)
    monkeypatch.delenv("GATEWAY_PORT", raising=False)
    captured: dict[str, object] = {}

    def fake_run(app_path: str, *, host: str, port: int, factory: bool) -> None:
        captured["app_path"] = app_path
        captured["host"] = host
        captured["port"] = port
        captured["factory"] = factory

    monkeypatch.setattr(gateway_main.uvicorn, "run", fake_run)
    gateway_main.main()
    assert captured == {
        "app_path": "apps.gateway.app:app",
        "host": "0.0.0.0",
        "port": 8080,
        "factory": False,
    }


def test_main_respects_gateway_env(monkeypatch) -> None:
    monkeypatch.setenv("GATEWAY_HOST", "127.0.0.1")
    monkeypatch.setenv("GATEWAY_PORT", "9090")
    captured: dict[str, object] = {}

    def fake_run(app_path: str, *, host: str, port: int, factory: bool) -> None:
        del app_path, factory
        captured["host"] = host
        captured["port"] = port

    monkeypatch.setattr(gateway_main.uvicorn, "run", fake_run)
    gateway_main.main()
    assert captured == {"host": "127.0.0.1", "port": 9090}
