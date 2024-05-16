from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The access key for the AWS account.'
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The secret key for the AWS account.',
        'secret': True
    },
    region_name={
        'type': ARG_TYPE.STR,
        'description': 'The AWS region where the S3 bucket is located.'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the S3 bucket.'
    },
    key={
        'type': ARG_TYPE.STR,
        'description': 'The key of the object to be queried.'
    },
    input_serialization={
        'type': ARG_TYPE.STR,
        'description': 'The format of the data in the object that is to be queried.'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='PCAQ2LJDOSWLNSQKOCPW',
    aws_secret_access_key='U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i',
    region_name='us-east-1',
    bucket='mindsdb-bucket',
    key='iris.csv',
    input_serialization="{'CSV': {'FileHeaderInfo': 'NONE'}}",
)
