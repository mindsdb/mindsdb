---
title: Semantic Search with RAG
sidebarTitle: RAG
---

This documentation describes the Retrieval Augmented Generation integration that can be used to create, train, and deploy models within MindsDB.

It supports the following:

* Large language models such as [OpenAI](https://docs.mindsdb.com/integrations/ai-engines/openai) and [Writer](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/writer_handler#readme).
* Vector databases such as [ChromaDB](https://docs.mindsdb.com/integrations/vector-db-integrations/chromadb) and FAISS.
* Embedding models compatible with the [Hugging Face sentence_transformers library](https://huggingface.co/sentence-transformers).

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use RAG within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the [OpenAI](https://help.openai.com/en/articles/4936850-where-do-i-find-my-openai-api-key) or [Writer](https://dev.writer.com/docs/quickstart#step-1-obtain-your-api-keys) API key. 

## Setup

Create an AI engine from the [RAG handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/rag_handler).
You can create a RAG engine using this command and providing either [OpenAI](https://docs.mindsdb.com/integrations/ai-engines/openai) or [Writer](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/writer_handler#readme) parameters:

```sql
CREATE ML_ENGINE rag_engine
FROM rag
USING
    openai_api_key="openai-api-key", 
    writer_org_id="writer-org", -- optional if openai is used
    writer_api_key="writer-api-key"; -- optional if openai is used
```

Create a model using the `rag_engine`:

```sql
CREATE MODEL rag_model
PREDICT answer
USING
   engine = 'rag_engine',
   llm_type = 'openai',                          -- choose one of OpenAI or Writer
   url = 'link-to-webpage',                      -- this is optional if the FROM cluse is provided
   vector_store_folder_name = 'db_connection',   -- provide a folder name that will/stores vector db data
   input_column = 'question';                       -- provide column name that stores the input to the model
```

Where:

| Name                       | Description                                                 |
|----------------------------|-------------------------------------------------------------|
| `llm_type`                 | It defines which LLM is used.                               |
| `url`                      | It is used to find the knowlege from a website.         |
| `vector_store_folder_name` | It is a folder name to which the vector db data in between sessions is persisted. |
| `input_column`             | It is a column name that stores the input to the model. |

<Info>
When creating a RAG model, it is required to provide data either in the `url` parameter or in the `FROM` clause.
</Info>

## Usage

The following examples illustrate various ways to integrate RAG with different data sources, including files, URLs, databases, and vector databases.

### From a URL

The following example utilize `rag_engine` to create a model with the `CREATE MODEL` statement.

```sql
CREATE ML_ENGINE rag_engine
FROM rag
USING
    openai_api_key = 'sk-xxx';
```

Create a model using this engine:

```sql
CREATE MODEL mindsdb_rag_model
predict answer
USING
   engine = "rag_engine",
   llm_type = "openai",
   url='https://docs.mindsdb.com/what-is-mindsdb',
   vector_store_folder_name = 'db_connection',
   input_column = 'question'; 
```

Check the status of the model.

```sql
DESCRIBE mindsdb_rag_model;
```

Now you can use the model to answer your questions.

```sql
SELECT *
FROM mindsdb_rag_model
WHERE question = 'What ML use cases does MindsDB support?';
```

On execution, we get:


| answer                                                                                                                                                                                       | source_documents | question                                |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------------------------|
|   MindsDB supports various machine learning use cases such as anomaly detection, forecasting, recommenders, classification, and regression. It also supports multimedia use cases such as video and text semantic search, text to audio, text to video, and text to image. | `{} `              | What ML use cases does MindsDB support? |


### From Database

The following example utilize `rag_engine` to create a model with the `CREATE MODEL` statement and `MySQL` database as a knowlege base.


```sql
CREATE ML_ENGINE rag_engine
FROM rag
USING
    openai_api_key = 'sk-xxx';
```

Connect to MySQL database:

```sql
CREATE DATABASE mysql_demo_db
WITH ENGINE = 'mysql',
PARAMETERS = {
    "user": "user",
    "password": "MindsDBUser123!",
    "host": "db-demo-data.cwoyhfn6bzs0.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "public"
};
```

Create a model using this engine and include the FORM clause:

```sql
CREATE MODEL rag_handler_db
FROM mysql_demo_db 
    (SELECT * FROM demo_fda_context LIMIT 2)
PREDICT answer
USING
   engine="rag",
   llm_type="openai",
   vector_store_folder_name='test_db',
   input_column='question';
```

Now you can use the model to answer your questions.

```sql
SELECT *
FROM rag_handler_db
WHERE question='what product is best for treating a cold?';
```

On execution, we get:

| answer                                                                                                                                                                                       | source_documents | question                                |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------------------------|
|   ShopRite Arthritis Pain Acetaminophen is not specifically designed for treating a cold. It may help to temporarily relieve minor aches and pains associated with a cold, but it is not the best product for treating a cold. It is always best to consult with a doctor or pharmacist for the most appropriate medication for treating a cold. | `{"column":["full_ingredients","indications_and_usage","intended_purpose_of_product","active_ingredient"],"sources_content":["ShopRite Arthritis  ..."],"sources_document":["dataframe"],"sources_row":[1,1,1,1]}`   | what product is best for treating a cold? |


### From File

The following example utilize `rag_engine` to create a model with the `CREATE MODEL` statement and  uploaded file as a knowlege base.


```sql
CREATE ML_ENGINE rag_engine
FROM rag
USING
    openai_api_key = 'sk-xxx';
```

Upload a [file](https://docs.mindsdb.com/mindsdb_sql/sql/create/file) using the GUI `Upload File` option. Create a model using this engine and include the FORM clause:

```sql
CREATE MODEL rag_handler_files
FROM files 
    (SELECT * FROM uploaded_file)
PREDICT answer
USING
   engine="rag",
   llm_type="openai",
   vector_store_folder_name='test_db',
   input_column='question';
```

Now you can use the model to answer your questions.

```sql
SELECT *
FROM rag_handler_files
WHERE question='what product is best for treating a cold?';
```

On execution, we get:

| answer                                                                                                                                                                                       | source_documents | question                                |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------------------------|
|   ShopRite Arthritis Pain Acetaminophen is not specifically designed for treating a cold. It may help to temporarily relieve minor aches and pains associated with a cold, but it is not the best product for treating a cold. It is always best to consult with a doctor or pharmacist for the most appropriate medication for treating a cold. | `{"column":["full_ingredients","indications_and_usage","intended_purpose_of_product","active_ingredient"],"sources_content":["ShopRite Arthritis  ..."],"sources_document":["dataframe"],"sources_row":[1,1,1,1]}`   | what product is best for treating a cold? |
