## Testing ChromaDB Handler with [FDA](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/fda.csv)

**1. Testing CREATE DATABASE**

To connect to a local running ChromaDB (e.g. in docker)

```
CREATE DATABASE chroma_dev
WITH ENGINE = "chromadb",
PARAMETERS = {
   "chroma_api_impl": "rest",
   "chroma_server_host": "localhost",
   "chroma_server_http_port": 8000,
   "embedding_function": "sentence-transformers/all-mpnet-base-v2" } -- if 'embedding_function' not specified, it used this embedding model by default
```

**2. Testing Insert data into ChromaDB collection**
```
create table chroma_dev.fda_10 (
select * from mysql_demo_db.demo_fda_context limit 10);
```


**3. Testing Count from ChromaDB collection**

```
select count(*) from chroma_dev.fda_context_10
```

**4. Selection from a ChromaDB collection**

```
SELECT *
FROM chroma_dev.fda_10
Limit 5
```


### Results

Drop a remark based on your observation.
- [x] Works Great 💚

---
