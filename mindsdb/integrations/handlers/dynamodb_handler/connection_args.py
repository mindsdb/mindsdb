from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The AWS access key that identifies the user or IAM role.',
        'required': True,
        'label': 'AWS Access Key'
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The AWS secret access key that identifies the user or IAM role.',
        'secret': True,
        'required': True,
        'label': 'AWS Secret Access Key'
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region to connect to.',
        'required': True,
        'label': 'AWS Region'
    },
    aws_session_token={
        'type': ARG_TYPE.STR,
        'description': 'The AWS session token that identifies the user or IAM role. This becomes necessary when using temporary security credentials.',
        'secret': True,
        'required': False,
        'label': 'AWS Session Token'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='PCAQ2LJDOSWLNSQKOCPW',
    aws_secret_access_key='U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i',
    region_name='us-east-1'
)
