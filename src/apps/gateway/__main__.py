"""Gateway composition root: uvicorn health stub on port 8080."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.environ.get("GATEWAY_HOST", "0.0.0.0")
    port = int(os.environ.get("GATEWAY_PORT", "8080"))
    uvicorn.run(
        "apps.gateway.app:app",
        host=host,
        port=port,
        factory=False,
    )


if __name__ == "__main__":
    main()
