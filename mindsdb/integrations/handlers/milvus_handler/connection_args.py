from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    uri={
        "type": ARG_TYPE.STR,
        "description": "uri of milvus service",
        "required": True,
    },
    token={
        "type": ARG_TYPE.STR,
        "description": "token to support docker or cloud service",
        "required": False,
    },
    search_default_limit={
        "type": ARG_TYPE.INT,
        "description": "default limit to be passed in select statements",
        "required": False,
    },
    search_metric_type={
        "type": ARG_TYPE.STR,
        "description": "metric type used for searches",
        "required": False,
    },
    search_ignore_growing={
        "type": ARG_TYPE.BOOL,
        "description": "whether to ignore growing segments during similarity searches",
        "required": False,
    },
    search_params={
        "type": ARG_TYPE.DICT,
        "description": "specific to the `search_metric_type`",
        "required": False,
    },
    create_auto_id={
        "type": ARG_TYPE.BOOL,
        "description": "whether to auto generate id when inserting records with no ID (default=False)",
        "required": False,
    },
    create_id_max_len={
        "type": ARG_TYPE.STR,
        "description": "maximum length of the id field when creating a table (default=64)",
        "required": False,
    },
    create_embedding_dim={
        "type": ARG_TYPE.INT,
        "description": "embedding dimension for creating table (default=8)",
        "required": False,
    },
    create_dynamic_field={
        "type": ARG_TYPE.BOOL,
        "description": "whether or not the created tables have dynamic fields or not (default=True)",
        "required": False,
    },
    create_content_max_len={
        "type": ARG_TYPE.INT,
        "description": "max length of the content column (default=200)",
        "required": False,
    },
    create_content_default_value={
        "type": ARG_TYPE.STR,
        "description": "default value of content column (default='')",
        "required": False,
    },
    create_schema_description={
        "type": ARG_TYPE.STR,
        "description": "description of the created schemas (default='')",
        "required": False,
    },
    create_alias={
        "type": ARG_TYPE.STR,
        "description": "alias of the created schemas (default='default')",
        "required": False,
    },
    create_index_params={
        "type": ARG_TYPE.DICT,
        "description": "parameters of the index created on embeddings column (default={})",
        "required": False,
    },
    create_index_metric_type={
        "type": ARG_TYPE.STR,
        "description": "metric used to create the index (default='L2')",
        "required": False,
    },
    create_index_type={
        "type": ARG_TYPE.STR,
        "description": "the type of index (default='AUTOINDEX')",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    uri="./milvus_local.db",
    token="",
    search_default_limit=100,
    search_metric_type="L2",
    search_ignore_growing=True,
    search_params={"nprobe": 10},
    create_auto_id=False,
    create_id_max_len=64,
    create_embedding_dim=8,
    create_dynamic_field=True,
    create_content_max_len=200,
    create_content_default_value="",
    create_schema_description="MindsDB generated table",
    create_alias="default",
    create_index_params={},
    create_index_metric_type="L2",
    create_index_type="AUTOINDEX",
)
