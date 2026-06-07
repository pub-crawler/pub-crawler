# These will run in order; make sure dependencies are in order

migrations = {
    "create_node_table": """
  CREATE TABLE node (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label VARCHAR(256) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
  );
  """,
    "create_edge_table": """
  CREATE TABLE edge (
    from_node INT NOT NULL,
    to_node INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_node, to_node),
    FOREIGN KEY (from_node) REFERENCES node (id) ON DELETE CASCADE,
    FOREIGN KEY (to_node) REFERENCES node (id) ON DELETE CASCADE
  );
  """,
    "create_node_property_table": """
  CREATE TABLE node_property (
    id INT NOT NULL,
    name VARCHAR(32) NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, name),
    FOREIGN KEY (id) REFERENCES node (id) ON DELETE CASCADE
  );
  """,
    "create_edge_property_table": """
  CREATE TABLE edge_property (
    from_node INT NOT NULL,
    to_node INT NOT NULL,
    name VARCHAR(32) NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (from_node, to_node, name),
    FOREIGN KEY (from_node, to_node) REFERENCES edge (from_node, to_node) ON DELETE CASCADE
  );
  """,
}


async def database_setup(connection):

    await _ensure_migrations(connection)

    applied = await _get_applied_migrations(connection)

    for name, ddl in migrations.items():
        if name not in applied:
            await _apply_migration(connection, name, ddl)


async def _ensure_migrations(connection):
    await connection.execute("""CREATE TABLE IF NOT EXISTS migrations (
    name VARCHAR(32) PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    )""")


async def _get_applied_migrations(connection):
    rows = await connection.fetch("SELECT name FROM migrations")
    return set(map(lambda row: row["name"], rows))


async def _apply_migration(connection, name, ddl):
    async with connection.transaction():
        await connection.execute(ddl)
        await connection.execute("INSERT INTO migrations (name) VALUES ($1)", name)
