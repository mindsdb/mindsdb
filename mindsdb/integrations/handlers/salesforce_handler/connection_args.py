from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    username={
        'type': ARG_TYPE.STR,
        'description': 'The username for the Salesforce account.',
        'required': True,
        'label': 'Username'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password for the Salesforce account.',
        'secret': True,
        'required': True,
        'label': 'Password'
    },
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'The client ID (consumer key) from a connected app in Salesforce.',
        'required': True,
        'label': 'Client ID (Consumer Key)'
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'The client secret (consumer secret) from a connected app in Salesforce.',
        'required': True,
        'label': 'Client Secret (Consumer Secret)'
    },
    is_sandbox={
        'type': ARG_TYPE.BOOL,
        'description': 'Set this to True if you need to connect to a sandbox, False for production environments. '
                       'If not provided defaults to False.',
        'required': False,
        'label': 'Is Sandbox'
    }
)

connection_args_example = OrderedDict(
    username='demo@example.com',
    password='demo_password',
    client_id='3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY',
    client_secret='5A52C1A1E21DF9012IODC9ISNXXAADDA9',
    is_sandbox=True
)
