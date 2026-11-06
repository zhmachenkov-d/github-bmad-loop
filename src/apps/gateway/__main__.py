"""Gateway composition root: uvicorn health stub on port 8080."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.environ.get("GATEWAY_HOST") or "0.0.0.0"
    raw_port = os.environ.get("GATEWAY_PORT") or "8080"
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise SystemExit("GATEWAY_PORT must be an integer") from exc
    uvicorn.run(
        "apps.gateway.app:app",
        host=host,
        port=port,
        factory=False,
    )


if __name__ == "__main__":
    main()
