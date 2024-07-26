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
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Amazon S3 bucket.',
        'required': True,
        'label': 'Amazon S3 Bucket'
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region to connect to. Default is `us-east-1`.',
        'required': False,
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
    aws_access_key_id='AQAXEQK89OX07YS34OP',
    aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    aws_session_token='FQoGZXIvYXdzEHcaDmJjJj...',
    region_name='us-east-2',
    bucket='my-bucket',
)
