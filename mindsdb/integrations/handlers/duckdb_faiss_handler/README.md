# DuckDB + Faiss Handler

## Using duckdb_faiss handler

This handler combines DuckDB for metadata storage and SQL filtering with Faiss for high-performance vector similarity search.


### 1. Create a FAISS Database and Knowledge Base

`duckdb_faiss` handler is installed by default with mindsdb.
If no shared pgvector defined (by using KB_PGVECTOR_URL env variable), duckdb_faiss database will be created by default with knowledbe base if `storage` parameter is not specified

```
CREATE KNOWLEDGE BASE kb_animals
USING 
  embedding_model = {"provider": "openai", "model_name": "text-embedding-3-small"};
```

You can create own duckdb_faiss manually as well:

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
- `persist_directory`: Optional custom storage path. If not set - a handler storage will be used
- `metric`: Distance metric - possible values: cosine/ip/l1/l2. Default is "cosine"
- `use_gpu`: Enable GPU acceleration (default: False)
- `nlist`: IVF parameter for clustering. Used as default value in create IVF index. Default is 1024
- `nprobe`: controls the number of clusters to search during a query. Default is 1


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

Important: t is not possible to create an index for empty FAISS knowledge base because both type of indexes require to having data in knowledge base before creating it. The loaded data is used to train index. The size of the train data and also count of clusters can affect on index quality.

Query:
```sql
CREATE INDEX ON KNOWLEDGE_BASE kb_animals
WITH (
  type='ivf_file' 
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

When duckdb_faiss table is crated the handler creates folder for it. It contains files:
- duckdb.db - a duckdb database to store metadata for knowledge base
- faiss_index - faiss index file
Folder name - is a table name

### Locks and concurrency

Because ivf and flat indexes are loaded in RAM and disk copy is used only to store changes in index (insert / delete records)
For small indexes: index is unloaded from RAM after request and loaded again before next request

When index became big read time increases: the index is cached in RAM and locked to prevent using it in different process / thread. If mindsdb is used from different threads processes - a `index file locked` exception might appear


### Checking resources

**RAM**
For indexes located in RAM, when data is inserted in faiss index - it forecast required memory and don't allow to insert if it exceeds available memory

**disk**
When index is created, it requires to get 2 or 3 more times disk space (depending on index type). The free disk space is also checked before staring to crate index
What occupies disk:
- an old faiss_index file (its backup)
- fetched vectors from old index
- a new index


### Mixed Search Optimizations
For queries that mix vectors and rich metadata:
- The handler estimates metadata selectivity (`COUNT(*) WHERE <filters>`) to choose the best execution plan.
- **Vector-first strategy** fetches an expanding set of candidates from FAISS until enough records satisfy the metadata filters.
- **Metadata-first strategy** constrains candidate IDs via DuckDB before scoring them in FAISS batches (`META_BATCH = 10,000`).




