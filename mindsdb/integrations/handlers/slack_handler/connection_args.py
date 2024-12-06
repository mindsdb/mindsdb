from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    token={
        'type': ARG_TYPE.STR,
        'description': 'The bot token for the Slack app.',
        'secret': True,
        'required': True,
        'label': 'Token',
    },
    app_token={
        'type': ARG_TYPE.PWD,
        'description': 'The app token for the Slack app.',
        'secret': True,
        'required': False,
        'label': 'App Token'
    }
)

connection_args_example = OrderedDict(
    token='xapp-A111-222-xyz',
    app_token='xoxb-111-222-xyz'
)
