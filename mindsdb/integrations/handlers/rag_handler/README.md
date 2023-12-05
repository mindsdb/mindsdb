# General RAG ML Handler

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?
A simplified interface for users to create and query Retrieval-Augmented Generation (RAG) models.

This handler supports:
- For LLMs:
    - OpenAI API
    - Writer API
- For vectorDBs:
    - ChromaDB
    - FAISS
- For Embedding models:
    - Any compatible model with HF `sentence_transformers` library

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

This integration is useful for a number of reasons:
- Makes it possible to query large document corpus that may exceed most LLM token limit by making use of an intermediate vectorDB, in this case ChromaDB.
- You can load in existing persisted ChromaDB with embeddings.
- Run Question and Answer queries against the powerful different LLM APIs.

An ideal use case would be long and complex business documents that you want to be able to ask questions about or alternatively academic papers or other long form texts of interest.

This integration is not suitable for other LLM tasks other than Question Answering.

## Are models created with this integration fast and scalable, in general?
The rate limiting step is the ingestion of data from regular Database table in text format to vector embeddings in ChromaDB collection or FAISS index. Depending on the Embedding model used, the size of the document corpus and your compute specification it can take a long time to complete!

Once you have ingested your data, the LLM API you choose to utilise is very fast and scalable when querying on a question and answer task.

## What are the recommended system specifications for models created with this framework?
Since we use hosted LLM APIs, there is no need for any additional system specifications. As noted above, the embedding step can take longer on less powerful hardware. The only real requirement is to have an internet connection.

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
Users are allowed complete control over the underlying framework by passing parameters via the USING syntax.

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
No, this integration doesn't support DESCRIBE syntax.

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No, fine-tuning is not supported.

## Are there any other noteworthy aspects to this handler?
The handler has a number of default parameters set, the user only needs to pass:

- `llm_type: str`
- llm auth parameters - depending on the llm_type, one of the following:
    - `open_ai_api_key: str`
    - `writer_api_key: str` and `writer_org_id: str`
- `vector_store_folder_name: str`

The other parameters have default values, see `settings.py` for more details.

## Any directions for future work in subsequent versions of the handler?
tbc

## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)

```sql
-- Create RAG engine NB you can provide api keys here or at create
-- You only need to provide key of llm you intend to use

create ML_Engine rag from rag
using
openai_api_key="openai-api-key",
writer_org_id="writer-org",
writer_api_key="writer-api-key";


--using DB table as input

CREATE MODEL rag_handler_db_test
FROM mysql_demo_db (select * from demo_fda_context limit 2)
PREDICT answer
USING
   engine="rag",
   llm_type="openai",
   vector_store_folder_name='test_db';

select * from information_schema.models where name ="rag_handler_db_test" ;


SELECT *
FROM rag_handler_db_test
WHERE question='what product is best for treating a cold?';


--using url as input

CREATE MODEL rag_handler_url_test
predict answer
USING
   engine="rag",
   llm_type="openai",
   url='https://docs.mindsdb.com/what-is-mindsdb',
   vector_store_folder_name='test_url';


SELECT *
FROM rag_handler_url_test
WHERE question='what ML use cases does mindsdb support?';

-- multiple urls as input
--upload files to UI first that contains a list of urls i.e. a csv with a column containing urls
--then use the following syntax

CREATE MODEL rag_handler_multi_url_test
FROM files (select * from file_with_urls)     
predict answer
USING
    engine="rag",
    llm_type="openai",
    url_column_name="url",
    vector_store_folder_name='test_multi_url';

SELECT *
FROM rag_handler_multi_url_test
WHERE question='answer to question?';

--using .txt or .pdf as input (first upload file using UI)

CREATE MODEL rag_handler_file_test
predict answer
from files (select * from uploaded_file)
USING
   engine="rag",
   llm_type="openai",
   vector_store_folder_name='test_file';

SELECT *
FROM rag_handler_file_test
WHERE question='what data types are supported?';

```
