from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    aws_access_key_id={
        "type": ARG_TYPE.STR,
        "description": "AWS access key ID (optional if using IAM role)",
        "required": False,
        "secret": True
    },
    aws_secret_access_key={
        "type": ARG_TYPE.STR,
        "description": "AWS secret access key (optional if using IAM role)",
        "required": False,
        "secret": True
    },
    aws_session_token={
        "type": ARG_TYPE.STR,
        "description": "AWS session token (optional)",
        "required": False,
        "secret": True
    },
    region_name={
        "type": ARG_TYPE.STR,
        "description": "AWS region (default: us-east-1)",
        "required": False,
    },
    vector_bucket={
        "type": ARG_TYPE.STR,
        "description": "S3 vector bucket name",
        "required": True,
    },
    dimension={
        "type": ARG_TYPE.INT,
        "description": "Vector dimensions for CREATE TABLE (default: 1536)",
        "required": False,
    },
    metric={
        "type": ARG_TYPE.STR,
        "description": "Distance metric: cosine/euclidean/dotproduct (default: cosine)",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    region_name="us-east-1",
    vector_bucket="my-vector-bucket",
    dimension=1536,
    metric="cosine",
)
