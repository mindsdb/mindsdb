from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    project_id={
        'type': ARG_TYPE.STR,
        'description': 'The GCP project id.'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The name of the GCS bucket.',
        'required': True,
        'label': 'GCS Bucket'
    },
    service_account_json_file_path={
        'type': ARG_TYPE.STR,
        'description': 'Path to the service account JSON file',
        'secret': True
    }
)


connection_args_example = OrderedDict(
    project_id='project_id',
    bucket='my-bucket',
    service_account_json_file_path='/Users/sam/Downloads/svc.json'
)
