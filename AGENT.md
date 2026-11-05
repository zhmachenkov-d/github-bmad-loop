# AGENT.md — github-bmad-loop

Instructions for agents working in this product repository.

## Layout

Hexagonal control plane (architecture spine Structural Seed):

- `src/core/` — domain types and (later) Run/Unit SMs
- `src/ports/` — protocols
- `src/adapters/postgres/` — Postgres JobStore (lease, apply, outbox)
- `src/apps/worker/` — leased worker composition root
- `src/apps/outbox/` — outbox dispatcher composition root
- `alembic/` — schema migrations
- `docker-compose.yml` — PostgreSQL 18 for local `dev`

## Tracer stub (story 1.1)

Stub Unit SM states live on **`units.state`**:

| State        | Meaning                                |
| ------------ | -------------------------------------- |
| `stub_ready` | Actionable; unit jobs may be leased    |
| `stub_done`  | Terminal for the tracer; not re-leased |

Transition: `stub_ready` → `stub_done` on event `stub_unit_complete`.

`jobs` is the lease/work queue only (`kind` ∈ `{run,unit}`, `ref_id`, lease columns, `version`). It does **not** store SM state.

Story **1.2** owns the real Unit SM table and lease-guarded apply API in `core`/ports. Replace the stub transition in `adapters/postgres/store.py` — do not invent a parallel apply path.

## Outbox (AD-16)

Idempotency key shape:

```text
aggregate_type:aggregate_id:effect_kind:dedupe_token
```

Tracer example: `unit:{unit_id}:stub_transition:{job_version_at_lease}`.

Inserts use `ON CONFLICT (idempotency_key) DO NOTHING`.

The stub dispatcher marks **`channel = 'stub'`** `pending` → `sent` with **no** GitHub/Slack I/O. Non-stub pending rows are left untouched. This is **not** the production outbox (no retry, dead-letter, or real senders — story 1.4+).

Integrity guards (story 1.1):

- `units.state` CHECK ∈ `{stub_ready, stub_done}`
- `UNIQUE (jobs.kind, jobs.ref_id)` — one leaseable job row per unit/run ref
- Apply clears a matching job lease on terminal no-op; guards `leased_by`, `kind`, `ref_id`; attributes event/outbox from locked `units.run_id`
- Worker exit codes: `0` applied, `1` lease lost, `2` idle / terminal no-op

## Deviations from `docs/ai_software_factory.md`

Handoff contracts for lease / atomic transition / outbox bind, with these mappings:

| Handoff idea              | Product shape                                          |
| ------------------------- | ------------------------------------------------------ |
| `work_orders` table       | **Not used.** Use `runs`, `units`, `jobs`              |
| SM state on work order    | Stub SM state on `units.state`                         |
| Lease on work_orders      | Lease columns on `jobs`                                |
| Outbox key format (old)   | AD-16 key shape above                                  |
| Repo layout `ai-factory/` | Structural Seed under `src/{core,ports,adapters,apps}` |

Do not copy the handoff `ai-factory/` tree verbatim.

## Local verify

Compose publishes Postgres only (bound to `127.0.0.1:5432`; default `bmad`/`bmad` is local-dev). Override with `DATABASE_URL` outside that compose stack. Worker and outbox stay host-side one-shots for the tracer.

Postgres 18 mounts the named volume at `/var/lib/postgresql` (not `/var/lib/postgresql/data`). After pulling that compose change, reset the old volume once: `docker compose down -v` (destroys local DB data), then `docker compose up -d`.

Everyday CI gates (ruff check, ruff format --check, mypy on `src/`, pytest with Postgres) live in `.github/workflows/ci.yml` on `pull_request` and `push`. Install the format-on-commit hook once; ruff check / mypy / pytest stay CI-only (not pre-commit hooks). Keep `.pre-commit-config.yaml` `rev` equal to the locked `ruff` version in `uv.lock`.

```bash
uv sync --frozen --extra dev
uv run pre-commit install          # ruff-format only; rev must match uv.lock ruff
uv run ruff check .
uv run ruff format --check .
uv run mypy                        # files=["src"]; non-strict
docker compose up -d
uv run alembic upgrade head
uv run python scripts/seed_dev.py  # truncates then seeds one cycle; --keep to append
uv run python -m apps.worker       # exit 0 applied / 1 lease lost / 2 idle
uv run python -m apps.outbox
uv run pytest
```

Stack pins: Python 3.14, PostgreSQL 18, sync psycopg 3.3.x, SQLAlchemy 2.x + Alembic.
