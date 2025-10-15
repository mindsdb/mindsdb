from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    metric={
        "type": ARG_TYPE.STR,
        "description": "Distance metric for vector similarity search. Options: 'cosine' or 'l2'.",
        "required": False,
        "label": "Distance Metric",
        "default": "cosine",
    },
    backend={
        "type": ARG_TYPE.STR,
        "description": "Faiss backend algorithm. Options: 'flat', 'ivf', 'hnsw'.",
        "required": False,
        "label": "Backend Algorithm",
        "default": "hnsw",
    },
    use_gpu={
        "type": ARG_TYPE.BOOL,
        "description": "Enable GPU acceleration for Faiss operations.",
        "required": False,
        "label": "Use GPU",
        "default": False,
    },
    nlist={
        "type": ARG_TYPE.INT,
        "description": "Number of clusters for IVF backend.",
        "required": False,
        "label": "IVF Clusters",
        "default": 1024,
    },
    nprobe={
        "type": ARG_TYPE.INT,
        "description": "Number of clusters to probe for IVF backend.",
        "required": False,
        "label": "IVF Probe",
        "default": 32,
    },
    hnsw_m={
        "type": ARG_TYPE.INT,
        "description": "HNSW connectivity parameter (number of bi-directional links).",
        "required": False,
        "label": "HNSW Connectivity",
        "default": 32,
    },
    hnsw_ef_search={
        "type": ARG_TYPE.INT,
        "description": "HNSW search parameter (size of dynamic candidate list).",
        "required": False,
        "label": "HNSW Search",
        "default": 64,
    },
    persist_directory={
        "type": ARG_TYPE.STR,
        "description": "Optional custom directory for persisting data. If not provided, uses MindsDB's handler storage.",
        "required": False,
        "label": "Persist Directory",
    },
)

connection_args_example = OrderedDict(
    metric="cosine",
    backend="hnsw",
    use_gpu=False,
    nlist=1024,
    nprobe=32,
    hnsw_m=32,
    hnsw_ef_search=64,
)
