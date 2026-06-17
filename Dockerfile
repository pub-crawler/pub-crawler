# syntax=docker/dockerfile:1

# ---- builder: resolve locked deps into /app/.venv ----
FROM python:3.13-slim-bookworm AS builder

# uv as a static binary; pin a digest/tag in CI if you want reproducibility
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Install only dependencies first so this layer caches across code changes.
# --no-install-project: this is a flat/virtual project (no build-system), so
# there is nothing to install but the deps.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

# ---- runtime: same base so the venv's interpreter symlinks stay valid ----
FROM python:3.13-slim-bookworm

WORKDIR /app

# venv first on PATH activates it; the deps live in /app/.venv.
# PYTHONPATH lets the relocated bin/ and fixup/ scripts import pub_crawler (and
# each other) when run as `python bin/foo.py` from /app.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app:/app/bin:/app/fixup"

COPY --from=builder /app/.venv /app/.venv

# Application code: the pub_crawler package, production entry points in bin/, and
# one-shot migrations in fixup/.
COPY pub_crawler /app/pub_crawler
COPY bin /app/bin
COPY fixup /app/fixup

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 app && chown -R app /app
USER app

# Long-lived service that drains the queue and grows the graph.
# Override the command for bin/run_migrations.py / bin/add_seeds.py / bin/snapshot.py.
CMD ["python", "bin/crawl.py"]
