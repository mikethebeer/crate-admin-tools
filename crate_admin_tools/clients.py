import asyncpg

from crate_admin_tools.aio import run


class DB:
    def __init__(self, hosts, pool_size=10):
        self.dsn = f"postgres://crate@{hosts}/doc"
        self.pool_size = pool_size
        self.pool = None

    async def _get_pool(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.dsn, min_size=self.pool_size, max_size=self.pool_size
            )
        return self.pool

    async def execute(self, stmt, args=None):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            if args:
                await conn.execute(stmt, *args)
            else:
                await conn.execute(stmt)
            print(f"run: {stmt}")

    async def fetch(self, stmt):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            return await conn.fetch(stmt)

    async def _close_pool(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    def close(self):
        run(self._close_pool)

    def __enter__(self):
        return self

    def __exit__(self, *exs):
        self.close()
