import asyncpg
from pub_crawler.database import database_setup
import asyncio
import uvloop
import os


async def run_migrations():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"])
    try:
        await database_setup(conn)
    finally:
        await conn.close()


if __name__ == "__main__":

    uvloop.install()
    asyncio.run(run_migrations())
