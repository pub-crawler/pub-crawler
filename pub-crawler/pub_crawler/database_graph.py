import orjson
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
from cachetools import LRUCache

DEFAULT_MAX_CACHE_SIZE = 200_000


class DatabaseGraph:
    def __init__(
        self, pool: asyncpg.Pool, *, max_cache_size: int = DEFAULT_MAX_CACHE_SIZE
    ) -> None:
        self._pool = pool
        self._cache = LRUCache(maxsize=max_cache_size)

    async def ensure_node(self, label: str) -> None:
        if label in self._cache:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO node (label) VALUES ($1)
                ON CONFLICT (label) DO NOTHING
                """,
                label,
            )

    async def ensure_nodes(self, labels: list[str]) -> None:
        to_upsert = list(filter(lambda l: l not in self._cache, labels))
        if not to_upsert:
            return
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO node (label)
                SELECT unnest($1::text[])
                ON CONFLICT (label) DO NOTHING
                """,
                to_upsert,
            )

    async def ensure_edge(self, from_label: str, to_label: str) -> None:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            await conn.execute(
                """
                INSERT INTO edge (from_node, to_node) VALUES ($1, $2) ON CONFLICT (from_node, to_node) DO NOTHING
                """,
                from_node,
                to_node,
            )

    async def ensure_from_edges(self, from_label: str, to_labels: list[str]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO edge (from_node, to_node)
                SELECT f.id, t.id
                FROM node f, node t
                WHERE f.label = $1 AND t.label = ANY($2::text[])
                ON CONFLICT (from_node, to_node) DO NOTHING
                """,
                from_label,
                to_labels,
            )

    async def ensure_to_edges(self, from_labels: list[str], to_label: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO edge (from_node, to_node)
                SELECT f.id, t.id
                FROM node f, node t
                WHERE f.label = ANY($1::text[]) AND t.label = $2
                ON CONFLICT (from_node, to_node) DO NOTHING
                """,
                from_labels,
                to_label,
            )

    async def has_node(self, label: str) -> bool:
        async with self._pool.acquire() as conn:
            return await self._node_id(conn, label) is not None

    async def has_edge(self, from_label: str, to_label: str) -> bool:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            ts = await conn.fetchval(
                "SELECT created_at FROM edge WHERE from_node = $1 AND to_node = $2",
                from_node,
                to_node,
            )
            return ts is not None

    async def delete_node(self, label: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM node WHERE label=$1
                """,
                label,
            )
            if label in self._cache:
                del self._cache[label]

    async def delete_edge(self, from_label: str, to_label: str) -> None:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            await conn.execute(
                "DELETE FROM edge WHERE from_node = $1 AND to_node = $2",
                from_node,
                to_node,
            )

    async def set_node_property(self, label: str, name: str, value: Any) -> None:
        async with self._pool.acquire() as conn:
            id = await self._node_id(conn, label)
            await conn.execute(
                """
            INSERT INTO node_property (id, name, value)
            VALUES ($1, $2, $3)
            ON CONFLICT (id, name) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            """,
                id,
                name,
                orjson.dumps(value).decode(),
            )

    async def set_nodes_property(
        self, labels: list[str], name: str, value: Any
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO node_property (id, name, value)
                SELECT n.id, $2 as name, $3 as value
                FROM node n
                WHERE n.label = ANY($1::text[])
                ON CONFLICT (id, name) DO UPDATE
                SET value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                """,
                labels,
                name,
                orjson.dumps(value).decode(),
            )

    async def set_node_properties(self, label: str, properties: dict[str, Any]) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO node_property (id, name, value)
                SELECT n.id, kv.key, kv.value
                FROM node n, jsonb_each($2::jsonb) AS kv
                WHERE n.label = $1
                ON CONFLICT (id, name) DO UPDATE
                SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
                """,
                label,
                orjson.dumps(properties).decode(),
            )

    async def set_edge_property(
        self, from_label: str, to_label: str, name: str, value: Any
    ) -> None:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            await conn.execute(
                """
                INSERT INTO edge_property (from_node, to_node, name, value)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (from_node, to_node, name) DO UPDATE
                SET value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
                """,
                from_node,
                to_node,
                name,
                orjson.dumps(value).decode(),
            )

    async def set_edge_properties(
        self, from_label: str, to_label: str, properties: dict[str, Any]
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO edge_property (from_node, to_node, name, value)
                SELECT f.id, t.id, kv.key, kv.value
                FROM node f, node t, jsonb_each($3::jsonb) AS kv
                WHERE f.label = $1
                AND t.label = $2
                ON CONFLICT (from_node, to_node, name) DO UPDATE
                SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
                """,
                from_label,
                to_label,
                orjson.dumps(properties).decode(),
            )

    async def set_from_edges_property(
        self, from_label: str, to_labels: list[str], name: str, value: Any
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO edge_property (from_node, to_node, name, value)
                SELECT f.id, t.id, $3, $4
                FROM node f, node t
                WHERE f.label = $1
                AND t.label = ANY($2::text[])
                ON CONFLICT (from_node, to_node, name) DO UPDATE
                SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
                """,
                from_label,
                to_labels,
                name,
                orjson.dumps(value).decode(),
            )

    async def set_to_edges_property(
        self, from_labels: list[str], to_label: str, name: str, value: Any
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO edge_property (from_node, to_node, name, value)
                SELECT f.id, t.id, $3, $4
                FROM node f, node t
                WHERE f.label = ANY($1::text[])
                AND t.label = $2
                ON CONFLICT (from_node, to_node, name) DO UPDATE
                SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
                """,
                from_labels,
                to_label,
                name,
                orjson.dumps(value).decode(),
            )

    async def get_node_property(self, label: str, name: str) -> Any:
        async with self._pool.acquire() as conn:
            id = await self._node_id(conn, label)
            value = await conn.fetchval(
                """
            SELECT value FROM node_property WHERE id = $1 AND name = $2
                """,
                id,
                name,
            )
            if value is None:
                return None
            else:
                return orjson.loads(value)

    async def get_nodes_property(self, labels: list[str], name: str) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT n.label as label, np.value as value
                FROM node n join node_property np on n.id = np.id
                WHERE n.label = ANY($1::text[])
                AND np.name = $2
                """,
                labels,
                name,
            )
            props = {}
            for row in rows:
                props[row["label"]] = orjson.loads(row["value"])
            return props

    async def get_edge_property(self, from_label: str, to_label: str, name: str) -> Any:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            value = await conn.fetchval(
                """
            SELECT value FROM edge_property
            WHERE from_node = $1
            AND to_node = $2
            AND name = $3
                """,
                from_node,
                to_node,
                name,
            )
            if value is None:
                return None
            else:
                return orjson.loads(value)

    async def get_node_properties(self, label: str) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            id = await self._node_id(conn, label)
            rows = await conn.fetch(
                """
            SELECT name, value FROM node_property
            WHERE id = $1
            """,
                id,
            )
            props = {}
            for row in rows:
                props[row["name"]] = orjson.loads(row["value"])
            return props

    async def get_edge_properties(
        self, from_label: str, to_label: str
    ) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            rows = await conn.fetch(
                """
            SELECT name, value FROM edge_property
            WHERE from_node = $1
            AND to_node = $2
            """,
                from_node,
                to_node,
            )
            props = {}
            for row in rows:
                props[row["name"]] = orjson.loads(row["value"])
            return props

    async def get_from_edges_property(
        self, from_label: str, to_labels: list[str], name: str
    ) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT t.label as label, ep.value as value
                FROM edge_property ep
                JOIN node f on ep.from_node = f.id
                JOIN node t on ep.to_node = t.id
                WHERE f.label = $1
                AND t.label = ANY($2::text[])
                AND ep.name = $3
                """,
                from_label,
                to_labels,
                name,
            )
            props = {}
            for row in rows:
                props[row["label"]] = orjson.loads(row["value"])
            return props

    async def get_to_edges_property(
        self, from_labels: list[str], to_label: str, name: str
    ) -> dict[str, Any]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.label as label, ep.value as value
                FROM edge_property ep
                JOIN node f on ep.from_node = f.id
                JOIN node t on ep.to_node = t.id
                WHERE f.label = ANY($1::text[])
                AND t.label = $2
                AND ep.name = $3
                """,
                from_labels,
                to_label,
                name,
            )
            props = {}
            for row in rows:
                props[row["label"]] = orjson.loads(row["value"])
            return props

    async def all_nodes(self) -> AsyncIterator[tuple[int, str, dict[str, Any]]]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                sql = """
                SELECT n.id, n.label,
                COALESCE(
                    jsonb_object_agg(p.name, p.value) FILTER (WHERE p.name IS NOT NULL),
                    '{}'
                ) AS props
                FROM node n
                LEFT JOIN node_property p ON p.id = n.id
                GROUP BY n.id
                """
                async for row in conn.cursor(sql):
                    yield row["id"], row["label"], orjson.loads(row["props"])

    async def all_edges(self) -> AsyncIterator[tuple[int, int, dict[str, Any]]]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                sql = """
                SELECT e.from_node, e.to_node,
                    COALESCE(
                        jsonb_object_agg(p.name, p.value) FILTER (WHERE p.name IS NOT NULL),
                        '{}'
                    ) AS props
                FROM edge e
                LEFT JOIN edge_property p
                    ON p.from_node = e.from_node AND p.to_node = e.to_node
                GROUP BY e.from_node, e.to_node
                """
                async for row in conn.cursor(sql):
                    yield row["from_node"], row["to_node"], orjson.loads(row["props"])

    async def _node_id(self, conn, label: str) -> int | None:
        if label in self._cache:
            return self._cache[label]
        else:
            id = await conn.fetchval("SELECT id FROM node WHERE label=$1", label)
            if id is not None:
                self._cache[label] = id
            return id
