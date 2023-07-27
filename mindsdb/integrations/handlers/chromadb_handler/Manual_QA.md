# Welcome to the MindsDB Manual QA Testing for Cassandra Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Cassandra Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
COMMAND THAT YOU RAN TO CREATE DATABASE.
```

![CREATE_DATABASE](Image URL of the screenshot)

**2. Testing CREATE PREDICTOR**

```
COMMAND THAT YOU RAN TO CREATE PREDICTOR.
```

![CREATE_PREDICTOR](Image URL of the screenshot)

**3. Testing SELECT FROM PREDICTOR**

```
COMMAND THAT YOU RAN TO DO A SELECT FROM.
```

![SELECT_FROM](Image URL of the screenshot)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

## Testing Cassandra Handler with [Home Rentals](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv)

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
