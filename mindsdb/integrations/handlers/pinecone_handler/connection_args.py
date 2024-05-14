from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "The API key that can be found in your pinecone account",
        "required": True,
        "secret": True
    },
    environment={
        "type": ARG_TYPE.STR,
        "description": "The environment name corresponding to the `api_key`",
        "required": True,
    },
    dimension={
        "type": ARG_TYPE.INT,
        "description": "dimensions of the vectors to be stored in the index (default=8)",
        "required": False,
    },
    metric={
        "type": ARG_TYPE.STR,
        "description": "distance metric to be used for similarity search (default='cosine')",
        "required": False,
    },
    pods={
        "type": ARG_TYPE.INT,
        "description": "number of pods for the index to use, including replicas (default=1)",
        "required": False,
    },
    replicas={
        "type": ARG_TYPE.INT,
        "description": "the number of replicas. replicas duplicate your index. they provide higher availability and throughput (default=1)",
        "required": False,
    },
    pod_type={
        "type": ARG_TYPE.STR,
        "description": "the type of pod to use, refer to pinecone documentation (default='p1')",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    api_key="00000000-0000-0000-0000-000000000000",
    environment="gcp-starter",
    dimension=8,
    metric="cosine",
    pods=1,
    replicas=1,
    pod_type='p1',
)
