from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    gcs_access_key_id={
        'type': ARG_TYPE.STR,
        'description': 'The HMAC access key id.',
        'required': True,
        'label': 'GCS Access Key'
    },
    gcs_secret_access_key={
        'type': ARG_TYPE.STR,
        'description': 'The HMAC secret access key.',
        'secret': True,
        'required': True,
        'label': 'GCS Secret Access Key'
    },
    service_account_keys={
        'type': ARG_TYPE.PATH,
        'description': 'Full path or URL to the service account JSON file',
        'secret': True
    },
    service_account_json={
        'type': ARG_TYPE.DICT,
        'description': 'Content of service account JSON file',
        'secret': True
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the Google CLoud Storage bucket.',
        'required': True,
        'label': 'GCS Bucket'
    },
    prefix={
        'type': ARG_TYPE.STR,
        'description': 'The prefix that used to filter blobs in the bucket.',
        'required': False
    },
    file_type={
        'type': ARG_TYPE.STR,
        'description': 'The extension of the files of the bucket.',
        'required': False
    },
)

connection_args_example = OrderedDict(
    gcs_access_key_id='AQAXEQK89OX07YS34OP',
    gcs_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    service_account_keys='/path/to/service_account.json',
    bucket='my-bucket',
    prefix='al',
    file_type='parquet'
)
