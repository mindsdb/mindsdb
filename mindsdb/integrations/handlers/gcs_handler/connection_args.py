from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the GCS bucket.',
        'label': 'GCS Bucket'
    },
    service_account_keys={
        'type': ARG_TYPE.PATH,
        'description': 'Path to the service account JSON file',
        'label': 'Path to the service account JSON file',
        'secret': True
    },
    service_account_json={
        'type': ARG_TYPE.DICT,
        'description': 'Content of service account JSON file',
        'label': 'Content of service account JSON file',
        'secret': True
    }
)


connection_args_example = OrderedDict(
    bucket='my-bucket',
    service_account_keys='/Users/sam/Downloads/svc.json'
)
