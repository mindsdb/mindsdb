from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The access key for the AWS account.',
        'required': True
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The secret key for the AWS account.',
        'secret': True,
        'required': True
    },
    aws_session_token={
        'type': ARG_TYPE.STR,
        'description': 'The session token for the AWS account.',
        'required': False
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region.',
        'required': False
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='PCAQ2LJDOSWLNSQKOCPW',
    aws_secret_access_key='U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i',
)
