import sqlite3
from mindsdb_native.libs.constants.mindsdb import DATA_TYPES, DATA_SUBTYPES
import re


def create_sqlite_db(path, data_frame):
    con = sqlite3.connect(path)
    data_frame.to_sql(name='data', con=con, index=False)
    con.close()


def cast_df_columns_types(df, stats):
    types_map = {
        DATA_TYPES.NUMERIC: {
            DATA_SUBTYPES.INT: 'int64',
            DATA_SUBTYPES.FLOAT: 'float64',
            DATA_SUBTYPES.BINARY: 'bool'
        },
        DATA_TYPES.DATE: {
            DATA_SUBTYPES.DATE: 'datetime64',       # YYYY-MM-DD
            DATA_SUBTYPES.TIMESTAMP: 'datetime64'   # YYYY-MM-DD hh:mm:ss or 1852362464
        },
        DATA_TYPES.CATEGORICAL: {
            DATA_SUBTYPES.SINGLE: 'category',
            DATA_SUBTYPES.MULTIPLE: 'category'
        },
        DATA_TYPES.FILE_PATH: {
            DATA_SUBTYPES.IMAGE: 'object',
            DATA_SUBTYPES.VIDEO: 'object',
            DATA_SUBTYPES.AUDIO: 'object'
        },
        DATA_TYPES.SEQUENTIAL: {
            DATA_SUBTYPES.ARRAY: 'object'
        },
        DATA_TYPES.TEXT: {
            DATA_SUBTYPES.SHORT: 'object',
            DATA_SUBTYPES.RICH: 'object'
        }
    }

    columns = [dict(name=x) for x in list(df.keys())]

    for column in columns:
        try:
            name = column['name']
            if stats[name].get('empty', {}).get('is_empty', False):
                new_type = types_map[DATA_TYPES.NUMERIC][DATA_SUBTYPES.INT]
            else:
                col_type = stats[name]['typing']['data_type']
                col_subtype = stats[name]['typing']['data_subtype']
                new_type = types_map[col_type][col_subtype]
            if new_type == 'int64' or new_type == 'float64':
                df[name] = df[name].apply(lambda x: x.replace(',', '.') if isinstance(x, str) else x)
            if new_type == 'int64':
                df = df.astype({name: 'float64'})
            df = df.astype({name: new_type})
        except Exception as e:
            print(e)
            print(f'Error: cant convert type of DS column {name} to {new_type}')

    return df


def parse_filter(key, value):
    result = re.search(r'filter(_*.*)\[(.*)\]', key)
    operator = result.groups()[0].strip('_') or 'like'
    field = result.groups()[1]
    operators_map = {
        'like': 'like',
        'in': 'in',
        'nin': 'not in',
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
        'eq': '=',
        'neq': '!='
    }
    if operator not in operators_map:
        return None
    operator = operators_map[operator]
    return {'field': field, 'value': value, 'operator': operator}


def prepare_sql_where(where):
    marks = {}
    if len(where) > 0:
        for i in range(len(where)):
            field = where[i]['field'].replace('"', '""')
            operator = where[i]['operator']
            value = where[i]['value']
            var_name = f'var{i}'
            if ' ' in field:
                field = f'"{field}"'
            if operator == 'like':
                marks[var_name] = '%' + value + '%'
            else:
                marks[var_name] = value
            where[i] = f'{field} {operator} :var{i}'
        where = 'where ' + ' and '.join(where)
    else:
        where = ''
    return where, marks


def get_sqlite_columns_names(cursor):
    cursor.execute('pragma table_info(data);')
    column_name_index = [x[0] for x in cursor.description].index('name')
    columns = cursor.fetchall()
    return [x[column_name_index] for x in columns]


def get_sqlite_data(db_path, where, limit, offset):
    where = [] if where is None else where

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    offset = '' if limit is None or offset is None else f'offset {offset}'
    limit = '' if limit is None else f'limit {limit}'

    columns_names = get_sqlite_columns_names(cur)
    where = [x for x in where if x['field'] in columns_names]
    where, marks = prepare_sql_where(where)

    count_query = ' '.join(['select count(1) from data', where])
    cur.execute(count_query, marks)
    rowcount = cur.fetchone()[0]

    query = ' '.join(['select * from data', where, limit, offset])
    cur.execute(query, marks)
    data = cur.fetchall()
    data = [dict(zip(columns_names, x)) for x in data]

    cur.close()
    con.close()

    return {
        'data': data,
        'rowcount': rowcount,
        'columns_names': columns_names
    }
