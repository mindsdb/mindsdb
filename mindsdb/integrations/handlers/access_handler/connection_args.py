from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    db_file={
        'type': ARG_TYPE.STR,
        'description': 'The database file where the data will be stored.'
    }
)

connection_args_example = OrderedDict(
    db_file='C:\\Users\\minurap\\Documents\\example_db.accdb'
)
