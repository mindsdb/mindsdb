from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    db_file={
        "type": ARG_TYPE.PATH,
        "description": "The full path to the Microsoft Access database file (.mdb or .accdb). On Windows, use absolute paths like C:\\Users\\username\\Documents\\database.accdb",
        "required": True,
        "label": "Database File Path",
    }
)

connection_args_example = OrderedDict(db_file="C:\\Users\\minurap\\Documents\\example_db.accdb")
