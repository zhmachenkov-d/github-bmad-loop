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

| State        | Meaning                                      |
| ------------ | -------------------------------------------- |
| `stub_ready` | Actionable; unit jobs may be leased          |
| `stub_done`  | Terminal for the tracer; not re-leased       |

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

The stub dispatcher marks `pending` → `sent` with **no** GitHub/Slack I/O. This is **not** the production outbox (no retry, dead-letter, or real senders — story 1.4+).

## Deviations from `docs/ai_software_factory.md`

Handoff contracts for lease / atomic transition / outbox bind, with these mappings:

| Handoff idea              | Product shape                                      |
| ------------------------- | -------------------------------------------------- |
| `work_orders` table       | **Not used.** Use `runs`, `units`, `jobs`           |
| SM state on work order    | Stub SM state on `units.state`                       |
| Lease on work_orders      | Lease columns on `jobs`                            |
| Outbox key format (old)   | AD-16 key shape above                              |
| Repo layout `ai-factory/` | Structural Seed under `src/{core,ports,adapters,apps}` |

Do not copy the handoff `ai-factory/` tree verbatim.

## Local verify

```bash
docker compose up -d
alembic upgrade head
python scripts/seed_dev.py
python -m apps.worker
python -m apps.outbox
python -m pytest
```

Stack pins: Python 3.14, PostgreSQL 18, sync psycopg 3.3.x, SQLAlchemy 2.x + Alembic.
