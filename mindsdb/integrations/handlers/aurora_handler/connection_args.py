from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Amazon Aurora DB cluster.'
    },
    password={
        'type': ARG_TYPE.PWD,
        'description': 'The password to authenticate the user with the Amazon Aurora DB cluster.',
        'secret': True
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the Amazon Aurora DB cluster.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the Amazon Aurora DB cluster.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the Amazon Aurora DB cluster. Must be an integer.'
    },
    db_engine={
        'type': ARG_TYPE.STR,
        'description': "The database engine of the Amazon Aurora DB cluster. This can take one of two values: 'mysql' or 'postgresql'. This parameter is optional, but if it is not provided, 'aws_access_key_id' and 'aws_secret_access_key' parameters must be provided"
    },
    aws_access_key_id={
        'type': ARG_TYPE.STR,
        'description': "The access key for the AWS account. This parameter is optional and is only required to be provided if the 'db_engine' parameter is not provided."
    },
    aws_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': "The secret key for the AWS account. This parameter is optional and is only required to be provided if the 'db_engine' parameter is not provided.",
        'secret': True
    },
)

connection_args_example = OrderedDict(
    db_engine='mysql',
    host='mysqlcluster.cluster-123456789012.us-east-1.rds.amazonaws.com',
    port=3306,
    user='root',
    password='password',
    database='database'
)
