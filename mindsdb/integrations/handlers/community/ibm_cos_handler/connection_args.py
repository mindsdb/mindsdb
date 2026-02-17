from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    cos_hmac_access_key_id={
        "type": ARG_TYPE.PWD,
        "description": "IBM COS HMAC Access Key ID.",
        "required": True,
        "label": "HMAC Access Key ID",
        "secret": True,
    },
    cos_hmac_secret_access_key={
        "type": ARG_TYPE.PWD,
        "description": "IBM COS HMAC Secret Access Key.",
        "required": True,
        "label": "HMAC Secret Access Key",
        "secret": True,
    },
    cos_endpoint_url={
        "type": ARG_TYPE.STR,
        "description": "IBM COS Endpoint URL (e.g., https://s3.eu-gb.cloud-object-storage.appdomain.cloud).",
        "required": True,
        "label": "Endpoint URL",
    },
    bucket={
        "type": ARG_TYPE.STR,
        "description": "IBM COS Bucket Name (Optional).",
        "required": False,
        "label": "Bucket Name",
    },
)

connection_args_example = OrderedDict(
    cos_hmac_access_key_id="YOUR_HMAC_ACCESS_KEY_ID",
    cos_hmac_secret_access_key="YOUR_HMAC_SECRET_ACCESS_KEY",
    cos_endpoint_url="https://s3.eu-gb.cloud-object-storage.appdomain.cloud",
    bucket="YOUR_BUCKET_NAME",
)
