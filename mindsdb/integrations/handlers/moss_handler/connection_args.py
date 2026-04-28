from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

connection_args = OrderedDict(
    project_id={
        "type": ARG_TYPE.STR,
        "description": "Moss project ID from the Moss Portal (portal.usemoss.dev)",
        "required": True,
    },
    project_key={
        "type": ARG_TYPE.PWD,
        "description": "Moss project key from the Moss Portal",
        "required": True,
        "secret": True,
    },
    alpha={
        "type": ARG_TYPE.STR,
        "description": (
            "Hybrid search weight between semantic and keyword search. "
            "0.0 = pure keyword (BM25), 1.0 = pure semantic, 0.8 = default"
        ),
        "required": False,
    },
)

connection_args_example = OrderedDict(
    project_id="your-project-id",
    project_key="moss_access_key_xxxxx",
    alpha="0.8",
)
