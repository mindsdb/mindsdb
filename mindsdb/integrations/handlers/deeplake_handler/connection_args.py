from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    dataset_path={
        "type": ARG_TYPE.STR,
        "description": "Path to Deep Lake dataset (local path, S3, GCS, Azure, or hub://org/dataset)",
        "required": True,
    },
    token={
        "type": ARG_TYPE.STR,
        "description": "Deep Lake token for authentication (required for cloud datasets)",
        "required": False,
        "secret": True,
    },
    org_id={
        "type": ARG_TYPE.STR,
        "description": "Organization ID for Deep Lake hub datasets",
        "required": False,
    },
    runtime={
        "type": ARG_TYPE.DICT,
        "description": "Runtime configuration (e.g., {'tensor_db': True} for managed tensor database)",
        "required": False,
    },
    read_only={
        "type": ARG_TYPE.BOOL,
        "description": "Whether to open dataset in read-only mode (default=False)",
        "required": False,
    },
    search_default_limit={
        "type": ARG_TYPE.INT,
        "description": "Default limit for similarity search queries (default=10)",
        "required": False,
    },
    search_distance_metric={
        "type": ARG_TYPE.STR,
        "description": "Distance metric for similarity search: 'l2', 'cosine', or 'max_inner_product' (default='cosine')",
        "required": False,
    },
    search_exec_option={
        "type": ARG_TYPE.STR,
        "description": "Search execution option: 'python', 'compute_engine', or 'tensor_db' (default='python')",
        "required": False,
    },
    create_overwrite={
        "type": ARG_TYPE.BOOL,
        "description": "Whether to overwrite existing dataset when creating tables (default=False)",
        "required": False,
    },
    create_embedding_dim={
        "type": ARG_TYPE.INT,
        "description": "Dimension of embedding vectors for new datasets (default=384)",
        "required": False,
    },
    create_max_chunk_size={
        "type": ARG_TYPE.INT,
        "description": "Maximum chunk size for storing data (default=1000)",
        "required": False,
    },
    create_compression={
        "type": ARG_TYPE.STR,
        "description": "Compression algorithm for data storage (default=None)",
        "required": False,
    },
)

connection_args_example = OrderedDict(
    dataset_path="./my_deeplake_db",
    token="your_deeplake_token",
    org_id="your_org_id",
    runtime={"tensor_db": False},
    read_only=False,
    search_default_limit=10,
    search_distance_metric="cosine",
    search_exec_option="python",
    create_overwrite=False,
    create_embedding_dim=384,
    create_max_chunk_size=1000,
    create_compression=None,
)
