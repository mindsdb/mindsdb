from sqlalchemy import exc


def schema_id(connection, schema_name=None):
    """Fetch the id for schema"""
    cur = connection.cursor()
    if schema_name is None:
        cur.execute("SELECT current_schema")
        schema_name = cur.fetchall()[0][0]

    query = f"""
                SELECT id
                FROM sys.schemas
                WHERE name = '{schema_name}'
            """

    cur.execute(query)

    try:
        schema_id = cur.fetchall()[0][0]
    except Exception:
        raise exc.InvalidRequestError(schema_name)

    return schema_id


def table_id(connection, table_name, schema_name=None):
    """Fetch the id for schema.table_name, defaulting to current schema if
    schema is None
    """

    schema_idm = schema_id(connection=connection, schema_name=schema_name)

    q = f"""
        SELECT id
        FROM sys.tables
        WHERE name = '{table_name}'
        AND schema_id = {schema_idm}
        """

    cur = connection.cursor()
    cur.execute(q)

    try:
        table_id = cur.fetchall()[0][0]
    except Exception:
        raise exc.NoSuchTableError(table_name)

    return table_id
