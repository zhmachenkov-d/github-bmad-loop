# github-bmad-loop

GitHub App + Slack BMAD orchestration factory. Control plane is PostgreSQL with leased workers and a transactional outbox.

## Quick start (dev)

```bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
alembic upgrade head
python scripts/seed_dev.py
python -m apps.worker
python -m apps.outbox
pytest
```

See `AGENT.md` for agent-oriented conventions and tracer stub notes.
