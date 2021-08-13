import re

from crate import client


async def get_all_tables(client, schema=None, table=None):
    result = await client.fetch(
        """
        select table_schema, table_name from information_schema.tables
        where table_schema not in ('sys', 'blob', 'pg_catalog', 'information_schema')
        order by table_schema, table_name
        """
    )
    s_re = re.compile(fr"^{schema}$")
    t_re = re.compile(fr"^{table}$")

    filtered = [row for row in result if s_re.match(row[0])]
    filtered = [row for row in filtered if t_re.match(row[1])]

    return filtered


async def get_all_schemas(client):
    result = await client.fetch(
        """
        select distinct(table_schema) from information_schema.tables
        where table_schema not in ('sys', 'blob', 'pg_catalog', 'information_schema')
        order by 1
        """
    )
    res = [schema[0] for schema in result]
    return res
