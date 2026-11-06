"""Gateway health stub: GET /health → 200; no ingress routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from apps.gateway.app import app


def test_health_returns_200() -> None:
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200


def test_unknown_path_is_not_ingress() -> None:
    client = TestClient(app)
    response = client.post("/slack/events", json={})
    assert response.status_code == 404
