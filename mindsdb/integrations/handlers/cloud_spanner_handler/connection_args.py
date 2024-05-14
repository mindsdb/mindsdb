from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    instance_id={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner instance identifier.',
    },
    database_id={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner database indentifier.',
    },
    project={
        'type': ARG_TYPE.STR,
        'description': 'The Cloud Spanner project indentifier.',
    },
    dialect={
        'type': ARG_TYPE.STR,
        'description': 'Dialect of the database',
        "required": False,
    },
    credentials={
        'type': ARG_TYPE.STR,
        'description': 'The Google Cloud Platform service account key in the JSON format.',
        'secret': True
    },
)

connection_args_example = OrderedDict(
    instance_id='test-instance', datbase_id='example-db', project='your-project-id'
)
