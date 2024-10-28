from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    cos_api_key_id={
        "type": ARG_TYPE.PWD,
        "description": "IBM COS API Key.",
        "required": True,
        "label": "API Key",
        "secret": True,
    },
    cos_service_instance_id={
        "type": ARG_TYPE.STR,
        "description": "IBM COS Service Instance ID (Resource Instance ID).",
        "required": True,
        "label": "Service Instance ID",
    },
    cos_endpoint_url={
        "type": ARG_TYPE.STR,
        "description": "IBM COS Endpoint URL (e.g., https://s3.us.cloud-object-storage.appdomain.cloud).",
        "required": True,
        "label": "Endpoint URL",
    },
    cos_location_constraint={
        "type": ARG_TYPE.STR,
        "description": "IBM COS Location Constraint corresponding to the endpoint.",
        "required": True,
        "label": "Location Constraint",
    },
)

connection_args_example = OrderedDict(
    cos_api_key_id="W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk",
    cos_service_instance_id="crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::",
    cos_endpoint_url="https://s3.us.cloud-object-storage.appdomain.cloud",
    cos_location_constraint="us-south",
)
