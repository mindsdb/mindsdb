from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    db_url={
        "type": ARG_TYPE.STR,
        "description": "Xata database url with region, database and, optionally the branch information",
        "required": True,
    },
    api_key={
        "type": ARG_TYPE.STR,
        "description": "personal Xata API key",
        "required": True,
        "secret": True
    },
    dimension={
        "type": ARG_TYPE.INT,
        "description": "default dimension of embeddings vector used to create table when using create (default=8)",
        "required": False,
    },
    similarity_function={
        "type": ARG_TYPE.STR,
        "description": "similarity function to use for vector searches (default=cosineSimilarity)",
        "required": False,
    }
)

connection_args_example = OrderedDict(
    db_url="https://...",
    api_key="abc_def...",
    dimension=8,
    similarity_function="l1"
)
