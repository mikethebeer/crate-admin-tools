from argh import arg

from crate_admin_tools.aio import async_run_many, run
from crate_admin_tools.clients import DB
from crate_admin_tools.schemas import get_all_tables


async def create_replicas_stmt(client, schema, table, replicas):
    filtered = await get_all_tables(client, schema, table)
    stmt = []
    for row in filtered:
        schema = row[0]
        table = row[1]
        stmt.append(
            f"""alter table "{schema}".{table} set (number_of_replicas = '{replicas}')"""
        )
    return stmt


async def async_replicas(client, schema, table, replicas):
    stmts = await create_replicas_stmt(client, schema, table, replicas)
    await async_run_many(client.execute, stmts, concurrency=5)
    print("done.")


@arg("replicas", default="0-1", help="The number of replicas")
@arg("-s", "--schema", default=r".*", help="The schema name pattern")
@arg("-t", "--table", default=r".*", help="The table name pattern")
def replicas(replicas, schema=None, table=None, hosts="localhost:4200"):
    with DB(hosts) as client:
        run(async_replicas, client, schema, table, replicas)
