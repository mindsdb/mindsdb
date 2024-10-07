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
        'description': 'The AWS region where the Athena tables are created.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Athena database.'
    },
    workgroup={
        'type': ARG_TYPE.STR,
        'description': 'The Athena Workgroup'
    },
    catalog={
        'type': ARG_TYPE.STR,
        'description': 'The AWS Data Catalog'
    },
    results_output_location={
        'type': ARG_TYPE.STR,
        'description': 'The Athena Query Results Output Location s3://bucket-path/athena-query-results'
    },
    check_interval={
        'type': ARG_TYPE.INT,
        'description': 'The interval in seconds to check Athena for query results. Default is 0 seconds.'
    }
)

connection_args_example = OrderedDict(
    aws_access_key_id='<default>',
    aws_secret_access_key='<default>',
    region_name='us-east-1',
    catalog='AwsDataCatalog',
    database='default',
    workgroup='primary',
    results_output_location='s3://<bucket>/athena-query-results/',
    check_interval=0
)
