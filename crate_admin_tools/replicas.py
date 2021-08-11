from crate_admin_tools.schemas import get_all_tables
from argh import arg
from crate import client
from crate_admin_tools.schemas import get_all_tables


@arg("replicas", default="0-1", help="The number of replicas")
@arg("-s", "--schema", default=r".*", help="The schema name pattern")
@arg("-t", "--table", default=r".*", help="The table name pattern")
def replicas(replicas, schema=None, table=None, hosts="localhost:4200"):
    conn = client.connect(hosts, username="crate")
    cursor = conn.cursor()
    filtered = get_all_tables(conn, schema, table)

    for row in filtered:
        schema = row[0]
        table = row[1]
        cursor.execute(
            f"""
            alter table "{schema}".{table} set (number_of_replicas = '{replicas}')
            """
        )
        print(f"schema: {schema}, table: {table}, replicas: {replicas}")

    conn.close()
    print("done.")
