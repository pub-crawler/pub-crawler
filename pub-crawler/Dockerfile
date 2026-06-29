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

# Drop Arrow components pyarrow bundles but the snapshot never loads, here in the
# builder so they never enter the runtime image (deleting after the COPY below
# would not shrink it — the bytes would persist in the lower layer). Only Flight
# is safe: it's a gRPC RPC stack reachable solely via `import pyarrow.flight`,
# which we never do. compute/dataset/acero/substrait stay — _parquet.so hard-links
# them, so removing any would break `import pyarrow.parquet`.
RUN PA=/app/.venv/lib/python3.13/site-packages/pyarrow \
    && rm -f "$PA"/libarrow_flight.so* "$PA"/_flight.*.so \
    && rm -rf "$PA"/flight

# ---- runtime: same base so the venv's interpreter symlinks stay valid ----
FROM python:3.13-slim-bookworm

WORKDIR /app

# venv first on PATH activates it; the deps live in /app/.venv.
# PYTHONPATH lets the relocated bin/ and fixup/ scripts import pub_crawler (and
# each other) when run as `python bin/foo.py` from /app.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app:/app/bin:/app/fixup"

# Create the unprivileged user before copying so the files are owned at copy
# time. A trailing `chown -R /app` would re-materialise the whole .venv into a
# duplicate layer (~200 MB); --chown on each COPY avoids that.
RUN useradd --create-home --uid 10001 app

COPY --chown=app:app --from=builder /app/.venv /app/.venv

# Application code: the pub_crawler package, production entry points in bin/, and
# one-shot migrations in fixup/.
COPY --chown=app:app pub_crawler /app/pub_crawler
COPY --chown=app:app bin /app/bin
COPY --chown=app:app fixup /app/fixup

USER app

# Long-lived service that drains the queue and grows the graph.
# Override the command for bin/run_migrations.py / bin/add_seeds.py / bin/snapshot.py.
CMD ["python", "bin/crawl.py"]
