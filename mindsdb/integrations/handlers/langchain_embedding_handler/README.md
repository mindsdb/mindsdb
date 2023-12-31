# Langchain Embedding Handler

## Overview

This is a bridge handler that brings all modules that are supported under
`langchain.embeddings` to MindsDB. Here is a [comprehensive list](https://python.langchain.com/docs/integrations/text_embedding/) of supported embeddings models in Langchain.

To use individual embedding modules, additional deps need to be installed.
If you failed to create an embedding model, use `DESCRIBE <model_name>` to check if missing dependencies.

## Usage

Create the ML engine
```sql
CREATE ML_ENGINE langchain_embedding
FROM langchain_embedding;
```

Create an embedding model
```sql
CREATE MODEL openai_embedding_model
PREDICT embeddings
-- the name of the column in the output data that contains the embedding vectors
USING
  engine = 'langchain_embedding',
  class = 'OpenAIEmbeddings', -- the name of the embedding class in langchain.embeddings, alternatively, you can also use 'openai' instead
  input_columns = ['content'], -- the name of the column in the input data that contains the text info. You can specify multiple columns if they all contain text info to be embeded
  openai_api_key = '<your_api_key>'  -- alternative set your env variable OPEN_AI_API_KEY on the machine that runs MindsDB

;
```
All arguments specified after the `USING` clause, except `engine` and `class` will be passed to the constructor when initializing the embedding models. Please refer to the detailed doc page for each embedding models in langchain for acceptable arguments. For example, here is the [doc for OpenAI](https://api.python.langchain.com/en/latest/embeddings/langchain.embeddings.openai.OpenAIEmbeddings.html).

Make a prediction for one row
```sql
SELECT
  content, embedding
FROM
  openai_embedding_model
WHERE
  content = 'A sample text to be embeded'
;

```

Make a batch prediction
```sql
CREATE DATABASE mysql_demo_db
WITH ENGINE = "mysql",
PARAMETERS = {
    "user": "user",
    "password": "MindsDBUser123!",
    "host": "db-demo-data.cwoyhfn6bzs0.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "public"
    };

CREATE VIEW amazon_review (
    SELECT review as content FROM mysql_demo_db.amazon_reviews LIMIT 10
);

SELECT
  content, m.embeddings
FROM
  amazon_review as s
JOIN
    openai_embedding_model as m;
```
