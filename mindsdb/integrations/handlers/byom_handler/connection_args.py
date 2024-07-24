from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    code={
        'type': ARG_TYPE.PATH,
        'description': 'The path to model code'
    },
    modules={
        'type': ARG_TYPE.PATH,
        'description': 'The path to model requirements'
    },
    mode={
        'type': ARG_TYPE.STR,
        'description': 'Mode of byom hander. It can be "custom_function" for using it as container for functions'
    }
)
