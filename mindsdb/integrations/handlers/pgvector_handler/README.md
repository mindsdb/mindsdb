# PGVector Handler

This is the implementation of the PGVector for MindsDB.

## PGVector

Open-source vector similarity search for Postgres

Store your vectors with the rest of your data. 

Supports:

exact and approximate nearest neighbor search
L2 distance, inner product, and cosine distance
any language with a Postgres client
Plus ACID compliance, point-in-time recovery, JOINs, and all of the other great features of Postgres

## Implementation

This handler uses `pgvector` python library to make use of the vector data type in postgres created from the pgvector extension

The required arguments to establish a connection are the same as a regular postgres connection:

* `host`: the host name or IP address of the postgres instance
* `port`: the port to use when connecting
* `database`: the database to connect to
* `user`: the user to connect as
* `password`: the password to use when connecting

## Usage

### Installing the pgvector extension

where you have postgres installed run the following commands to install the pgvector extension

`cd /tmp
git clone --branch v0.4.4 https://github.com/pgvector/pgvector.git
cd pgvector
make
make install`

### Installing the pgvector python library
Ensure you install all from requirements.txt in the pgvector_handler folder

### Creating a database connection in MindsDB

You can create a database connection like you would for a regular postgres database, the only difference is that you need to specify the engine as `pgvector`

```sql
CREATE DATABASE pvec
WITH
    ENGINE = 'pgvector',
    PARAMETERS = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "postgres",
    "user": "user",
    "password": "password"
    };
```

You can insert data into a new collection like so

```sql
CREATE TABLE pvec.embed
    (SELECT embeddings FROM mysql_demo_db.test_embeddings
);

CREATE ML_ENGINE openai
FROM openai
USING
    api_key = 'your-openai-api-key';

CREATE MODEL openai_emb 
PREDICT embedding 
USING    
  engine = 'openai',
  model_name='text-embedding-ada-002',    
  mode = 'embedding',    
  question_column = 'review'; 

create table pvec.itemstest (
SELECT m.embedding AS embeddings, t.review content FROM  mysql_demo_db.amazon_reviews t
  join openai_emb  m
);

```

You can query a collection within your PGVector as follows:

```sql
SELECT *
FROM pvec.embed
Limit 5;

SELECT *
FROM pvec.itemstest
Limit 5;
```


You can query on semantic search like so:

```sql
SELECT *
FROM pvec3.items_test
WHERE embeddings = (select * from mindsdb.embedding) LIMIT 5;
```
