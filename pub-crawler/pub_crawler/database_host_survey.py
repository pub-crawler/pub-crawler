import orjson
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
from cachetools import LRUCache

DEFAULT_MAX_CACHE_SIZE = 200_000


class DatabaseHostSurvey:
    def __init__(
        self, pool: asyncpg.Pool, *, max_cache_size: int = DEFAULT_MAX_CACHE_SIZE
    ) -> None:
        self._pool = pool
        self._host_cache = LRUCache(maxsize=max_cache_size)

    async def ensure_host(self, hostname: str) -> None:
        if hostname in self._host_cache:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO host (hostname)
                SELECT $1::varchar
                WHERE NOT EXISTS (SELECT 1 FROM host h WHERE h.hostname = $1::varchar)
                ON CONFLICT (hostname) DO NOTHING
                """,
                hostname,
            )

    async def ensure_hosts(self, hostnames: list[str]) -> None:
        to_upsert = list(filter(lambda l: l not in self._host_cache, hostnames))
        if not to_upsert:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO host (hostname)
                SELECT arg_hostname FROM unnest($1::text[]) AS arg_hostname
                WHERE NOT EXISTS (SELECT 1 FROM host h WHERE h.hostname = arg_hostname)
                ON CONFLICT (hostname) DO NOTHING;
                """,
                to_upsert,
            )

    async def has_host(self, hostname: str) -> bool:
        async with self._pool.acquire() as conn:
            return await self._host_id(conn, hostname) is not None

    async def delete_host(self, hostname: str) -> None:
        async with self._pool.acquire() as conn:
            id = await self._host_id(conn, hostname)
            await conn.execute(
                """
                DELETE FROM host WHERE id=$1
                """,
                id,
            )
            if hostname in self._host_cache:
                del self._host_cache[hostname]

    async def get_host_property(self, hostname: str, name: str) -> Any:
        async with self._pool.acquire() as conn:
            id = await self._host_id(conn, hostname)
            value = await conn.fetchval(
                """
            SELECT value FROM host_property WHERE id = $1 AND name = $2
                """,
                id,
                name,
            )
            if value is None:
                return None
            else:
                return orjson.loads(value)

    async def get_host_properties(self, hostname: str) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            id = await self._host_id(conn, hostname)
            rows = await conn.fetch(
                """
            SELECT name, value FROM host_property
            WHERE id = $1
            """,
                id,
            )
            props = {}
            for row in rows:
                props[row["name"]] = orjson.loads(row["value"])
            return props

    async def get_hosts_property(
        self, hostnames: list[str], name: str
    ) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT h.hostname as hostname, hp.value as value
                FROM host h join host_property hp on h.id = hp.id
                WHERE h.hostname = ANY($1::text[])
                AND hp.name = $2
                """,
                hostnames,
                name,
            )
            props = {}
            for row in rows:
                props[row["hostname"]] = orjson.loads(row["value"])
            return props

    async def set_host_property(self, hostname: str, name: str, value: Any) -> None:
        async with self._pool.acquire() as conn:
            id = await self._host_id(conn, hostname)
            await conn.execute(
                """
            INSERT INTO host_property (id, name, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (id, name) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            """,
                id,
                name,
                orjson.dumps(value).decode(),
            )

    async def set_host_properties(
        self, hostname: str, properties: dict[str, Any]
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO host_property (id, name, value)
                SELECT h.id, kv.key, kv.value
                FROM host h, jsonb_each($2::jsonb) AS kv
                WHERE h.hostname = $1
                ON CONFLICT (id, name) DO UPDATE
                SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
                """,
                hostname,
                orjson.dumps(properties).decode(),
            )

    async def set_hosts_property(
        self, hostnames: list[str], name: str, value: Any
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO host_property (id, name, value)
                SELECT h.id, $2 as name, $3 as value
                FROM host h
                WHERE h.hostname = ANY($1::text[])
                ON CONFLICT (id, name) DO UPDATE
                SET value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                hostnames,
                name,
                orjson.dumps(value).decode(),
            )

    async def all_hosts(self) -> AsyncIterator[tuple[int, str, dict[str, Any]]]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                sql = """
                SELECT h.id, h.hostname,
                COALESCE(
                    jsonb_object_agg(p.name, p.value) FILTER (WHERE p.name IS NOT NULL),
                    '{}'
                ) AS props
                FROM host h
                LEFT JOIN host_property p ON p.id = h.id
                GROUP BY h.id
                """
                async for row in conn.cursor(sql):
                    yield row["id"], row["hostname"], orjson.loads(row["props"])

    async def delete_host_properties(self, hostname: str, names: list[str]) -> None:
        async with self._pool.acquire() as conn:
            id = await self._host_id(conn, hostname)
            await conn.execute(
                """
                DELETE FROM host_property
                WHERE id=$1
                AND name = ANY($2::text[])
                """,
                id,
                names,
            )

    async def _host_id(self, conn, hostname: str) -> int | None:
        if hostname in self._host_cache:
            return self._host_cache[hostname]
        else:
            id = await conn.fetchval("SELECT id FROM host WHERE hostname=$1", hostname)
            if id is not None:
                self._host_cache[hostname] = id
            return id
