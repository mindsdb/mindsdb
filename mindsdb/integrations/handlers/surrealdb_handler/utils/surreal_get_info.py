def table_names(connection):
    query = "INFO for DB"
    dict_1 = connection.query(query)
    dict_2 = dict_1['tb']
    tables = list(dict_2.keys())
    return tables


def column_info(connection, table):
    query = "INFO FOR TABLE " + table
    dict_1 = connection.query(query)
    dict_2 = dict_1['fd']
    columns = list(dict_2.keys())
    types = []

    for value in dict_2.values():
        a = value.split('TYPE ', 1)[1]
        type = a.split()[0]
        types.append(type)
    return columns, types
