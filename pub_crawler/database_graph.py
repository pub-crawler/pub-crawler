import json
from cachetools import LRUCache

DEFAULT_MAX_CACHE_SIZE = 200_000


class DatabaseGraph:
    def __init__(self, pool, *, max_cache_size=DEFAULT_MAX_CACHE_SIZE):
        self._pool = pool
        self._cache = LRUCache(maxsize=max_cache_size)

    async def ensure_node(self, label):
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

    async def ensure_edge(self, from_label, to_label):
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

    async def has_node(self, label):
        async with self._pool.acquire() as conn:
            return await self._node_id(conn, label) is not None

    async def has_edge(self, from_label, to_label):
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            ts = await conn.fetchval(
                "SELECT created_at FROM edge WHERE from_node = $1 AND to_node = $2",
                from_node,
                to_node,
            )
            return ts is not None

    async def delete_node(self, label):
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM node WHERE label=$1
                """,
                label,
            )
            if label in self._cache:
                del self._cache[label]

    async def delete_edge(self, from_label, to_label):
        async with self._pool.acquire() as conn:
            from_node = await self._node_id(conn, from_label)
            to_node = await self._node_id(conn, to_label)
            await conn.execute(
                "DELETE FROM edge WHERE from_node = $1 AND to_node = $2",
                from_node,
                to_node,
            )

    async def set_node_property(self, label, name, value):
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
                json.dumps(value),
            )

    async def set_edge_property(self, from_label, to_label, name, value):
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
                json.dumps(value),
            )

    async def get_node_property(self, label, name):
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
                return json.loads(value)

    async def get_edge_property(self, from_label, to_label, name):
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
                return json.loads(value)

    async def get_node_properties(self, label):
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
                props[row["name"]] = json.loads(row["value"])
            return props

    async def get_edge_properties(self, from_label, to_label):
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
                props[row["name"]] = json.loads(row["value"])
            return props

    async def all_nodes(self):
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
                    yield row["id"], row["label"], json.loads(row["props"])

    async def all_edges(self):
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
                    yield row["from_node"], row["to_node"], json.loads(row["props"])

    async def _node_id(self, conn, label):
        if label in self._cache:
            return self._cache[label]
        else:
            id = await conn.fetchval("SELECT id FROM node WHERE label=$1", label)
            if id is not None:
                self._cache[label] = id
            return id
