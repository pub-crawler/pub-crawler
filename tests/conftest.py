"""Shared fixtures for the non-live test suites."""

import os

import pytest
import uvloop
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def pytest_asyncio_loop_factories(config, item):
    """Run the async suite on uvloop, matching the loop the bin/ scripts use."""
    return {"uvloop": uvloop.new_event_loop}


@pytest.fixture(scope="session")
def pg_dsn():
    """DSN for the Postgres the DatabaseGraph contract tests run against.

    Point TEST_DATABASE_URL at a *dedicated* test database — the schema (applied
    by the `graph` db fixture via database_setup) is committed there. Per-test
    isolation is the caller's transaction rollback, so test rows never persist,
    but the schema does.

    Returns the DSN string only; the actual async connect/setup happens inside
    the async `graph` fixture (a live event loop), not here. Skips the db-marked
    tests (rather than failing) when TEST_DATABASE_URL is unset or the db code
    isn't importable yet, so the default DB-free suite is unaffected.
    """
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set")
    try:
        import asyncpg  # noqa: F401
    except ImportError as exc:
        pytest.skip(f"db harness not ready ({exc})")
    return dsn


@pytest.fixture(scope="session")
def keypair():
    """(private_pem_str, public_key_object) — one fresh RSA-2048 pair per run."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return pem, key.public_key()
