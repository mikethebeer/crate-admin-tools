from argh import arg

from crate_admin_tools.aio import async_run_many, run
from crate_admin_tools.clients import DB
from crate_admin_tools.schemas import get_all_tables

async def create_optimize_stmt(client, schema, table):
    filtered = await get_all_tables(client, schema, table)
    stmt = []
    for row in filtered:
        schema = row[0]
        table = row[1]
        stmt.append(
            f"""optimize table "{schema}"."{table}" """
        )
    return stmt


async def async_optimize(client, schema, table):
    stmts = await create_optimize_stmt(client, schema, table)
    await async_run_many(client.execute, stmts, concurrency=1)
    print("done.")


@arg("-s", "--schema", default=r".*", help="The schema name pattern")
@arg("-t", "--table", default=r".*", help="The table name pattern")
def optimize(schema=None, table=None, hosts="localhost:5432"):
    with DB(hosts, pool_size=20) as client:
        run(async_optimize, client, schema, table)
