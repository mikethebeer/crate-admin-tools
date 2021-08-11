import re
from crate import client


def get_all_tables(conn, schema=None, table=None):
    cursor = conn.cursor()
    cursor.execute(
        """
        select table_schema, table_name from information_schema.tables
        where table_schema not in ('sys', 'blob', 'pg_catalog', 'information_schema')
        order by table_schema, table_name
        """
    )
    result = cursor.fetchall()
    s_re = re.compile(fr"^{schema}$")
    t_re = re.compile(fr"^{table}$")

    filtered = [row for row in result if s_re.match(row[0])]
    filtered = [row for row in filtered if t_re.match(row[1])]

    return filtered


def get_all_schemas(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        select distinct(table_schema) from information_schema.tables
        where table_schema not in ('sys', 'blob', 'pg_catalog', 'information_schema')
        order by 1
        """
    )
    result = cursor.fetchall()
    res = [schema[0] for schema in result]
    return res
