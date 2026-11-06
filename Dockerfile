# Single app image for migrate / worker / outbox / gateway (AD-7).
FROM python:3.14-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /usr/local/bin/uv

WORKDIR /app

# Project metadata first for better layer caching.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
COPY scripts ./scripts

RUN uv sync --frozen --no-dev \
 && useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin app \
 && chown -R app:app /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

USER app

# Compose overrides `command` per service.
CMD ["python", "-m", "apps.worker"]
