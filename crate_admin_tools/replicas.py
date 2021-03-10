from argh import arg
from crate import client
import re


@arg("replicas", default="0-1", help="The number of replicas")
@arg("-s", "--schema", default=r".*", help="The schema name pattern")
@arg("-t", "--table", default=r".*", help="The table name pattern")
def replicas(replicas, schema=None, table=None, hosts="localhost:4200"):
    conn = client.connect(hosts, username="crate")
    cursor = conn.cursor()

    cursor.execute(
        """
        select table_schema, table_name from information_schema.tables
        where table_schema not in ('sys', 'blob', 'pg_catalog', 'information_schema')
        order by table_schema, table_name
        """
    )
    s_re = re.compile(fr"^{schema}$")
    t_re = re.compile(fr"^{table}$")
    result = cursor.fetchall()
    filtered = [row for row in result if s_re.match(row[0])]
    filtered = [row for row in filtered if t_re.match(row[1])]

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
