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

## Stub Unit SM (story 1.2)

States on **`units.state`** (CHECK widened in migration `0003`):

| State           | Meaning                                              |
| --------------- | ---------------------------------------------------- |
| `stub_ready`    | Actionable; `lease_next` claims only this state      |
| `stub_running`  | Work in progress under a held lease                  |
| `stub_paused`   | Paused under a held lease                            |
| `stub_done`     | Terminal success; lease cleared on entry             |
| `stub_failed`   | Terminal failure; lease cleared on entry             |

Declared edges (anything else → `IllegalTransitionError`):

| From            | Event            | To              | Outbox effect   |
| --------------- | ---------------- | --------------- | --------------- |
| `stub_ready`    | `stub_start`     | `stub_running`  | —               |
| `stub_running`  | `stub_complete`  | `stub_done`     | `stub_effect`   |
| `stub_running`  | `stub_pause`     | `stub_paused`   | `stub_notify`   |
| `stub_paused`   | `stub_resume`    | `stub_running`  | —               |
| `stub_running`  | `stub_fail`      | `stub_failed`   | `stub_effect`   |

Normative table and errors live in `src/core/stub_unit.py`. `jobs` is the lease/work queue only (`kind` ∈ `{run,unit}`, `ref_id`, lease columns, `version`); it does **not** store SM state.

**Apply (AD-12 subset):** `JobStorePort.apply_transition(lease, worker_id, event, result=None)` → `ApplyTransitionResult` with `job_version_after`. One transaction on the store-held connection: lease/version/`lease_expires_at > now()`/`leased_by` guards → enum state → table lookup. Guard order: `LeaseLostError` → `InvalidUnitStateError` → `IllegalTransitionError`. Non-`None` `result` → `UnsupportedResultError` before any write (story 1.2: always omit `result`).

On success: bump `jobs.version` every apply; **retain** `lease_id` / `leased_by` / `lease_expires_at` on non-terminal targets; **clear** lease only on `stub_done` / `stub_failed`. Append `event_log` with `event_type` equal to the SM event string; JSONB `payload` keys `job_version` (pre-bump), `lease_id`, `effect`, `result` (always `null`). Optional outbox row when the edge carries an effect.

**Worker (tracer):** one lease → `stub_start` → `dataclasses.replace(lease, job_version=…)` → `stub_complete`. Exit codes: `0` full cycle applied, `1` apply error (lease/SM), `2` idle (empty queue). No terminal no-op.

## Outbox (AD-16)

Idempotency key shape:

```text
aggregate_type:aggregate_id:effect_kind:dedupe_token
```

Stub examples: `unit:{unit_id}:stub_effect:{job_version_at_apply}` or `…:stub_notify:…` (pre-bump version observed on the lease).

Inserts use `ON CONFLICT (idempotency_key) DO NOTHING`.

The stub dispatcher marks **`channel = 'stub'`** `pending` → `sent` with **no** GitHub/Slack I/O. Non-stub pending rows are left untouched. This is **not** the production outbox (no retry, dead-letter, or real senders — story 1.4+).

Integrity guards:

- `units.state` CHECK ∈ stub enum (see table above)
- `UNIQUE (jobs.kind, jobs.ref_id)` — one leaseable job row per unit/run ref
- Apply guards `leased_by`, `kind`, `ref_id`; attributes event/outbox from locked `units.run_id`
- Outbox body on enqueue: `{effect, from_state, to_state, unit_id}` with `effect` ∈ `{stub_effect, stub_notify}`

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

Everyday CI gates (ruff check, ruff format --check, mypy on `src/`, pytest with Postgres) live in `.github/workflows/ci.yml` on `pull_request` and `push`. Install the format-on-commit hook once; ruff check / mypy / pytest stay CI-only (not pre-commit hooks). Keep `.pre-commit-config.yaml` `rev` equal to the locked `ruff` version in `uv.lock`. Pre-commit stays **ruff format only** — Semgrep and mutmut are not hooks.

Quality bar CI (AD-15 / R16) lives in `.github/workflows/quality-pr.yml` (PR/push Semgrep + diff mutmut) and `.github/workflows/quality-mutmut-schedule.yml` (Monday / `workflow_dispatch` full mutmut). Glue under `.ci/` is not package code. Owner Slack on worsen uses `SLACK_BOT_TOKEN` + `SLACK_CHANNEL_ID` via `chat.postMessage` (not a webhook); missing secrets skip notify without failing the job.

```bash
uv sync --frozen --extra dev
uv run pre-commit install          # ruff-format only; rev must match uv.lock ruff
uv run ruff check .
uv run ruff format --check .
uv run mypy                        # files=["src"]; non-strict
docker compose up -d
uv run alembic upgrade head
uv run python scripts/seed_dev.py  # truncates then seeds one cycle; --keep to append
uv run python -m apps.worker       # exit 0 cycle / 1 apply error / 2 idle
uv run python -m apps.outbox
uv run pytest
```

### Local verify quality

```bash
# Semgrep (AST-diff / baseline compare)
uv run python .ci/run_semgrep.py
uv run python .ci/run_semgrep.py --baseline-commit <base-sha>
uv run python .ci/run_semgrep.py --compare-baseline
# intentional bump only: uv run python .ci/run_semgrep.py --write-baseline

# Mutmut (diff survivors do not fail; full: score drop ≥ 1 or survived/no_tests growth fails)
uv run python .ci/run_mutmut_diff.py
uv run python .ci/run_mutmut_diff.py --base-ref <base-sha>
uv run python .ci/run_mutmut_full.py --compare-baseline
# intentional bump only: uv run python .ci/run_mutmut_full.py --write-baseline

# HITL worsen fixture (dry-run Slack payload; no secrets required)
uv run python .ci/run_worsen_fixture.py
```

Baseline bump rule: see `.ci/README.md` — only edit `.ci/baselines/` when intentionally accepting a new Semgrep finding or a lower mutmut score / higher survived or no_tests count. Hypothesis is a `dev` dependency for when property-based tests are earned later; not required on every change.

Stack pins: Python 3.14, PostgreSQL 18, sync psycopg 3.3.x, SQLAlchemy 2.x + Alembic.
