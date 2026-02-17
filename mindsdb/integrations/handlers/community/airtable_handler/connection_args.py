from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    base_id={
        'type': ARG_TYPE.STR,
        'description': 'The Airtable base ID.'
    },
    table_name={
        'type': ARG_TYPE.STR,
        'description': 'The Airtable table name.'
    },
    api_key={
        'type': ARG_TYPE.STR,
        'description': 'The API key for the Airtable API.',
        'secret': True
    }
)

connection_args_example = OrderedDict(
    base_id='dqweqweqrwwqq',
    table_name='iris',
    api_key='knlsndlknslk'
)
