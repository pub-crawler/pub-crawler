import json


class DatabaseGraph:
    def __init__(self, conn):
        self._conn = conn

    async def ensure_node(self, label):
        await self._conn.execute(
            """
            INSERT INTO node (label) VALUES ($1) ON CONFLICT (label) DO NOTHING
            """,
            label,
        )

    async def ensure_edge(self, from_label, to_label):
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        await self._conn.execute(
            """
            INSERT INTO edge (from_node, to_node) VALUES ($1, $2) ON CONFLICT (from_node, to_node) DO NOTHING
            """,
            from_node,
            to_node,
        )

    async def has_node(self, label):
        return await self._node_id(label) is not None

    async def has_edge(self, from_label, to_label):
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        ts = await self._conn.fetchval(
            "SELECT created_at FROM edge WHERE from_node = $1 AND to_node = $2",
            from_node,
            to_node,
        )
        return ts is not None

    async def delete_node(self, label):
        await self._conn.execute(
            """
            DELETE FROM node WHERE label=$1
            """,
            label,
        )

    async def delete_edge(self, from_label, to_label):
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        await self._conn.execute(
            "DELETE FROM edge WHERE from_node = $1 AND to_node = $2",
            from_node,
            to_node,
        )

    async def set_node_property(self, label, name, value):
        id = await self._node_id(label)
        await self._conn.execute(
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
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        await self._conn.execute(
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
        id = await self._node_id(label)
        value = await self._conn.fetchval(
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
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        value = await self._conn.fetchval(
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
        id = await self._node_id(label)
        rows = await self._conn.fetch(
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
        from_node = await self._node_id(from_label)
        to_node = await self._node_id(to_label)
        rows = await self._conn.fetch(
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
        async with self._conn.transaction():
            async for row in self._conn.cursor(sql):
                yield row["id"], row["label"], json.loads(row["props"])

    async def all_edges(self):
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
        async with self._conn.transaction():
            async for row in self._conn.cursor(sql):
                yield row["from_node"], row["to_node"], json.loads(row["props"])

    async def _node_id(self, label):
        return await self._conn.fetchval("SELECT id FROM node WHERE label=$1", label)
