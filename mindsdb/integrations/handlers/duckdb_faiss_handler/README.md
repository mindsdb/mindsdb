# DuckDB + Faiss Handler

## Using duckdb_faiss handler

This handler combines DuckDB for metadata storage and SQL filtering with Faiss for high-performance vector similarity search.


### 1. Create a FAISS Database and Knowledge Base

`duckdb_faiss` handler is installed by default with mindsdb. When the `storage` parameter is not specified it creates default vector storage. It can be:
- pgvector (if the KB_PGVECTOR_URL env variable is defined)
- otherwise, a duckdb_faiss database will be created by default 

Create knowledge base with default vector db:
```
CREATE KNOWLEDGE BASE kb_animals
USING 
  embedding_model = {"provider": "openai", "model_name": "text-embedding-3-small"};
```

You can create your own duckdb_faiss database manually as well:

```sql
CREATE DATABASE mindsdb_faiss
WITH ENGINE = 'duckdb_faiss';
PARAMETERS = {
    "persist_directory": "/data/faiss_db_location",
    "metric": "ip",
    "use_gpu": false,
    "nlist": 10,
    "nprobe": 2
}
```

And use in knowledge base:
```sql
CREATE KNOWLEDGE BASE kb_animals
USING 
  storage = mindsdb_faiss.animals_table, 
  embedding_model = {"provider": "openai", "model_name": "text-embedding-3-small"};
```

Parameters for duckdb_faiss database:
- `persist_directory`: Optional, custom storage path. If not set - a handler storage will be used
- `metric`: Optional, distance metric - possible values: cosine/ip/l1/l2. Default is "cosine"
- `use_gpu`: Optional, enable GPU acceleration (default: False)
- `nlist`: Optional, IVF parameter for clustering. Used as default value in create IVF index. Default is 1024
- `nprobe`: Optional, controls the number of clusters to search during a query. Default is 1


### 2. Insert data

The same as for other vector storages, insert from select or from values: 
```sql
INSERT INTO kb_animals (id, content, legs)
VALUES (1, 'duck', 2), (2, 'cat', 4);
```

### 3. Querying the Knowledge Base

**Vector similarity search**
```sql
SELECT * FROM kb_animals
WHERE content = 'cat' and distance < 0.5
```

**Mixed search**
```sql
SELECT * FROM kb_animals
WHERE content = 'cat' AND legs = 4  
```
Supported `LIKE`, `NOT LIKE`, `>`, `>=`, `<`, `<=` filters for metadata columns


**Hybrid search**
```sql
SELECT * FROM kb_animals
WHERE content = 'cat' AND legs = 4  
  AND hybrid_search = TRUE;
```

Can be used with bool `hybrid_search` or float `hybrid_search_alpha` parameters


## 4. Create FAISS Indexes

When a new duckdb_faiss is created, it starts from using [flat FAISS index](https://faiss.ai/cpp_api/struct/structfaiss_1_1IndexFlat.html). It works by scanning all index file to get similar vectors. Also a flat index is located in RAM, and its size is restricted by available memory. 
To speed up vector search you can convert to other type of indexes. Available options:
- ivf - [Inverted File](https://faiss.ai/cpp_api/struct/structfaiss_1_1IndexIVF.html). It is also located in memory, but faster than FLAT
- ivf_file, the same as ivf, but located on disk and don't require to be loaded into RAM. This type of indexes isn't supported on windows

Important: It is not possible to create an index for an empty FAISS knowledge base because both types of indexes require data in the knowledge base before creating it. The loaded data is used to train the index. The size of the training data and the number of clusters can affect index quality.

Query:
```sql
CREATE INDEX ON KNOWLEDGE_BASE kb_animals
WITH (
  type = 'ivf_file',
  nlist = 100,
  train_count = 10000
);
```

Parameters:
- `type` - optional, default is ivf_file
  - for windows default is the 'ivf'
- `nlist` optional, number of clusters for IVF, default 1024,
- `train_count` optional, number of vectors to use for training, default is calculated from nlist.
 

## Implementation details

### How it works

When a duckdb_faiss table is created, the handler creates a folder for it. It contains:
- duckdb.db - a duckdb database to store metadata for knowledge base
- faiss_index - faiss index file
Folder name - is a table name

The other files in folders in faiss table:
- duckdb.db* - all files related to duckdb (duckdb.db.wal)
- faiss_index* - all files related faiss index (partitions, merged index for ivf_file)  
- dump/ - temporal folder for extracted vectors
- recover/ - temporal folder for index backup

### Locks and concurrency

Because IVF and FLAT indexes are loaded in RAM and the disk copy is used only to store changes in the index (insert/delete records), small indexes are unloaded from RAM after each request and loaded again before the next request.

When the index becomes large the read time increases, so the index is cached in RAM and locked to prevent using it in different processes or threads. If mindsdb is used from different threads or processes, an `index file locked` exception might appear. The lock is releases when handler cache is cleared (default timeout is 1 min)

Because insert from select into knowledge base is performed in background - the background process can't use faiss index if is locked by a gui. The implemented workaround is:
- before the query is sent into background
  - search all locks for vector bases of KBs in query and unload faiss database from cache
- after executing query in background
  - do the same (unload faiss database from cache)

Also locks prevent to insert into knowledge base in threads. This query won't work:
```sql
INSERT INTO my_kb SELECT * FROM db1.table1
USING threads=10
```


Important: faiss index isn't locked on windows, faiss library can write locked file there

### Checking resources

**RAM**
For indexes located in RAM, when data is inserted into the FAISS index it forecasts the required memory and does not allow the insert if it exceeds available memory.
This check is run after every 10k records inserted.

**disk**
When an index is created, it requires two to three times more disk space (depending on the index type). The free disk space is also checked before starting to create the index.
What occupies disk:
- an old faiss_index file (its backup)
- fetched vectors from old index
- a new index

### Keyword search



### Mixed search optimizations
For queries that mix vectors and rich metadata:
- The handler estimates metadata selectivity (`COUNT(*) WHERE <filters>`) to choose the best execution plan.
- **Vector-first strategy** fetches an expanding set of candidates from FAISS until enough records satisfy the metadata filters.
- **Metadata-first strategy** constrains candidate IDs via DuckDB before scoring them in FAISS batches (`META_BATCH = 10,000`).



