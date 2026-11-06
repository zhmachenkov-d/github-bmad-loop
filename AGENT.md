# AGENT.md — github-bmad-loop

Instructions for agents working in this product repository.

## Layout

Hexagonal control plane (architecture spine Structural Seed):

- `src/core/` — domain types and (later) Run/Unit SMs
- `src/ports/` — protocols
- `src/adapters/postgres/` — Postgres JobStore (lease, apply, outbox claim/outcomes)
- `src/adapters/outbox/` — ChannelRouter + stub `stub`/`github`/`slack` senders (no real API I/O)
- `src/apps/worker/` — leased worker composition root (continuous lease + in-process reconcile)
- `src/apps/outbox/` — outbox dispatcher composition root
- `src/apps/gateway/` — FastAPI health stub only (`GET /health`); no Slack/GitHub/panel/signal ingress
- `alembic/` — schema migrations
- `docker-compose.yml` — same topology locally and on the shared factory host: `postgres`, one-shot `migrate`, `worker`, `outbox`, `gateway`

## Stub Unit SM (story 1.2)

States on **`units.state`** (CHECK widened in migration `0003`):

| State          | Meaning                                          |
| -------------- | ------------------------------------------------ |
| `stub_ready`   | Actionable; claimable when lease null/expired    |
| `stub_running` | In progress; reclaimable when lease null/expired |
| `stub_paused`  | Paused; never claimed by `lease_next`            |
| `stub_done`    | Terminal success; lease cleared on entry         |
| `stub_failed`  | Terminal failure; lease cleared on entry         |

Declared edges (anything else → `IllegalTransitionError`):

| From           | Event           | To             | Outbox effect |
| -------------- | --------------- | -------------- | ------------- |
| `stub_ready`   | `stub_start`    | `stub_running` | —             |
| `stub_running` | `stub_complete` | `stub_done`    | `stub_effect` |
| `stub_running` | `stub_pause`    | `stub_paused`  | `stub_notify` |
| `stub_paused`  | `stub_resume`   | `stub_running` | —             |
| `stub_running` | `stub_fail`     | `stub_failed`  | `stub_effect` |

Normative table and errors live in `src/core/stub_unit.py`. `jobs` is the lease/work queue only (`kind` ∈ `{run,unit}`, `ref_id`, lease columns, `version`); it does **not** store SM state.

**Apply (AD-12 subset):** `JobStorePort.apply_transition(lease, worker_id, event, result=None)` → `ApplyTransitionResult` with `job_version_after`. One transaction on the store-held connection: lease/version/`lease_expires_at > now()`/`leased_by` guards → enum state → table lookup. Guard order: `LeaseLostError` → `InvalidUnitStateError` → `IllegalTransitionError`. Allowed `result`: `None` or stuck why `"attempts"` / `"deadline"` (written into `event_log.payload.result`); any other shape → `UnsupportedResultError` before any write.

On success: bump `jobs.version` every apply; **retain** `lease_id` / `leased_by` / `lease_expires_at` on non-terminal targets; **clear** lease only on `stub_done` / `stub_failed`. Append `event_log` with `event_type` equal to the SM event string; JSONB `payload` keys `job_version` (pre-bump), `lease_id`, `effect`, `result`. Optional outbox row when the edge carries an effect.

**Lease pool:** `JobStorePort.lease_next(worker_id)` claims one job from the shared `kind∈{run,unit}` pool (`FOR UPDATE SKIP LOCKED`, default TTL 90s, injectable). Unit claimable when `stub_ready` **or** `stub_running` with lease null/expired — never `stub_paused` / terminal. Run claimable when lease null/expired **and** (`version = 0` or `lease_id IS NOT NULL`) — stub stand-in so a released run is not re-claimed while expired mid-hold runs stay reclaimable. `heartbeat(job_id, lease_id, worker_id)` extends `lease_expires_at` by TTL on match (0 rows → handler stops, no further apply/release). `release(job_id, lease_id)` clears lease columns only when `lease_id` still matches.

**Worker loop:** continuous poll by default (idle sleep ≤1s); `--once` does one claim+dispatch then exit. Dispatch by `kind`: unit `stub_ready` → `stub_start` then `stub_complete` (with `replace` version/state refresh + heartbeat between); reclaimed `stub_running` → heartbeat then `stub_complete` only (heartbeat loss → stop, no further apply/release); run → no-op `release` (no Unit SM / event_log / outbox). Stub run claim skips jobs already released after a successful claim (so the loop does not starve units); expired mid-hold run leases remain reclaimable until Run SM exists. Exit codes for `--once`: `0` dispatched, `1` apply/handler error, `2` idle. Continuous mode: non-idle handler `1` ends the process; SIGINT/`KeyboardInterrupt` releases any in-flight lease then exits `0`.

**Reconcile tick (story 1.5):** in-process on the worker (default every 60s in continuous mode; `--reconcile-once` for one-shot). Under `pg_try_advisory_lock(42)` (miss → no-op; never blocks the lease loop): (1) grace force-clear leases with `lease_expires_at < now() - 5 minutes` on non-terminal unit work / held run leases (bump `jobs.version`, no `units.state` change); (2) `stub_running` past `attempts`/`deadline_at` → steal short reconcile lease → `apply_transition(stub_fail, result=why)` + stuck-alert outbox (`unit:{id}:stuck_alert:{pre_steal_version}`); run-kind breach → clear held lease + stuck alert (no Unit SM); (3) `status='dead'` outbox → one stub dead-alert (`outbox:{id}:dead_alert:1`). `jobs.attempts` / `max_attempts` / `deadline_at` are fixture/seed columns only (no runtime attempts writer this story).

## Outbox (AD-16)

Idempotency key shape:

```text
aggregate_type:aggregate_id:effect_kind:dedupe_token
```

Stub examples: `unit:{unit_id}:stub_effect:{job_version_at_apply}` or `…:stub_notify:…` (pre-bump version observed on the lease).

Inserts use `ON CONFLICT (idempotency_key) DO NOTHING`.

**Dispatcher (story 1.4):** continuous poll by default (idle sleep ≤2s); `--once` claims one due batch then exits. Claim: `status='pending' AND next_attempt_at <= now()`, all channels, `ORDER BY id`, `FOR UPDATE SKIP LOCKED`, `LIMIT` default 20. Route by `channel` to stub senders (`stub` / `github` / `slack`) — **no** real GitHub/Slack API I/O; unknown channels fail closed as permanent → `dead`. Success → `sent` + `external_id` + `sent_at`. `TransientError` → bump `attempts`; if new attempts `>= 10` (`OUTBOX_MAX_ATTEMPTS`) → `dead`, else stay `pending` with `next_attempt_at = now() + min(2^attempts, 300)` seconds. `PermanentError` → `dead` immediately. Never write `status='failed'`. Send-time idempotency: stub `github`/`slack` keep an in-memory key→`external_id` store (second `send` same key returns the same id, no second effect). Enqueue shape unchanged (AD-16). Stub senders are **not** production delivery.

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

## Secrets / env bootstrap

Required env **names** (values never committed; see `.env.example`):

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `DATABASE_URL` — pre-encoded URL for Compose apps (`migrate` / `worker` / `outbox` / `gateway`); use host `postgres`. Host-side debug may use `127.0.0.1` instead. Percent-encode special characters in user/password.
- `WORKER_ID`

Copy `.env.example` → `.env`, fill values, never commit `.env`. Empty values fail closed (`${VAR:?…}` in Compose). Laptop defaults such as `bmad`/`bmad` are local-dev only — **not** shared-host / prod posture.

Do **not** build app `DATABASE_URL` by interpolating `POSTGRES_*` in Compose — set a single pre-encoded `DATABASE_URL` in `.env` (Compose DNS host `postgres` for services).

## Local verify

Documented bring-up is Compose-first (AD-7): one topology for local and shared host.

```bash
cp .env.example .env   # fill POSTGRES_* / WORKER_ID / DATABASE_URL (host postgres for Compose)
docker compose up -d --build
docker compose ps      # migrate exited 0; postgres/worker/outbox/gateway running
curl -sf http://127.0.0.1:8080/health
docker compose run --rm worker python scripts/seed_dev.py
# poll until stub cycle lands (timeout ~60s):
for i in $(seq 1 30); do
  state=$(docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT state FROM units ORDER BY created_at DESC LIMIT 1;"' || true)
  status=$(docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT status FROM outbox ORDER BY id DESC LIMIT 1;"' || true)
  if [ "$state" = "stub_done" ] && [ "$status" = "sent" ]; then echo "ok: $state / $status"; break; fi
  sleep 2
done
# expect unit state stub_done and outbox status sent
```

Postgres 18 mounts the named volume at `/var/lib/postgresql` (not `/var/lib/postgresql/data`). After pulling that compose change, reset the old volume once: `docker compose down -v` (destroys local DB data), then `docker compose up -d --build`.

### Host-side debug only

Host `uv run` / `--once` is **debug only**. Cutover, shared-host path, and Verify evidence use Compose services only.

```bash
uv sync --frozen --extra dev
uv run pre-commit install          # ruff-format only; rev must match uv.lock ruff
uv run ruff check .
uv run ruff format --check .
uv run mypy                        # files=["src"]; non-strict
uv run alembic upgrade head        # only when debugging against host Postgres
uv run python scripts/seed_dev.py
uv run python -m apps.worker --once  # exit 0 dispatched / 1 error / 2 idle
uv run python -m apps.worker --reconcile-once  # one reconcile tick; 0 locked / 2 lock miss
uv run python -m apps.worker         # continuous poll + 60s reconcile until stopped
uv run python -m apps.outbox --once  # exit 0 processed / 2 idle; claim→send batch
uv run python -m apps.outbox         # continuous poll until stopped
uv run python -m apps.gateway        # health stub on :8080
uv run pytest                        # uses DATABASE_URL from the environment / repo `.env` (loaded by conftest when present; does not override already-set env)
```

Everyday CI gates (ruff check, ruff format --check, mypy on `src/`, pytest with Postgres) live in `.github/workflows/ci.yml` on `pull_request` and `push`. Install the format-on-commit hook once; ruff check / mypy / pytest stay CI-only (not pre-commit hooks). Keep `.pre-commit-config.yaml` `rev` equal to the locked `ruff` version in `uv.lock`. Pre-commit stays **ruff format only** — Semgrep and mutmut are not hooks.

Quality bar CI (AD-15 / R16) lives in `.github/workflows/quality-pr.yml` (PR/push Semgrep + diff mutmut) and `.github/workflows/quality-mutmut-schedule.yml` (Monday / `workflow_dispatch` full mutmut). Glue under `.ci/` is not package code. Owner Slack on worsen uses `SLACK_BOT_TOKEN` + `SLACK_CHANNEL_ID` via `chat.postMessage` (not a webhook); missing secrets skip notify without failing the job.

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

## Deploy (shared factory host)

Persistent founding-team Linux host (not laptop, not throwaway Codespace-as-factory). Same Compose topology — no profiles.

1. Clone or pull this repo on the host.
2. `cp .env.example .env` and fill non-laptop credentials (`POSTGRES_*`, `WORKER_ID`, pre-encoded `DATABASE_URL` with host `postgres`; do not reuse default laptop `bmad`/`bmad` as shared-host posture). Empty values fail closed at `docker compose up`.
3. Bring-up (migrate completes before apps start):

```bash
docker compose up -d --build
docker compose ps
# migrate: exited (0); postgres / worker / outbox / gateway: running
docker compose logs migrate   # alembic upgrade head
curl -sf http://127.0.0.1:8080/health
```

4. Seed one stub cycle (Compose path — preferred for Verify evidence):

```bash
docker compose run --rm worker python scripts/seed_dev.py
# poll until stub cycle lands (timeout ~60s):
for i in $(seq 1 30); do
  state=$(docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT state FROM units ORDER BY created_at DESC LIMIT 1;"' || true)
  status=$(docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT status FROM outbox ORDER BY id DESC LIMIT 1;"' || true)
  if [ "$state" = "stub_done" ] && [ "$status" = "sent" ]; then echo "ok: $state / $status"; break; fi
  sleep 2
done
```

Expect `stub_done` and `sent`.

### HITL cutover checklist (fill on shared host — no secrets)

Implementer ships empty slots. A person fills these on the founding-team shared host before this story is done. Do **not** record passwords, keys, or tokens here. Optional evidence may live in a private PR/issue instead.

1. Hostname + non-secret access path:
   - _(empty)_
2. Migrate / alembic head result:
   - _(empty)_
3. Seeded cycle: unit `stub_done` + outbox `sent` (runbook commands above):
   - _(empty)_
