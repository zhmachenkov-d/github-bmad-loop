# github-bmad-loop

GitHub App + Slack BMAD orchestration factory. Control plane is PostgreSQL with leased workers and a transactional outbox.

## Quick start (dev)

Requires **Python 3.14**, **uv**, and **Docker Compose**. Canonical agent/CI commands live in `AGENT.md`.

```bash
uv sync --frozen --extra dev
uv run pre-commit install
docker compose up -d
uv run alembic upgrade head
uv run python scripts/seed_dev.py
uv run python -m apps.worker
uv run python -m apps.outbox
uv run pytest
```

If Postgres fails to start after a compose pull that changed the `pgdata` mount, reset once with `docker compose down -v` and bring the stack back up.
