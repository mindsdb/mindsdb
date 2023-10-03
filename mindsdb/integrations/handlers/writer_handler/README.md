# Writer ML Handler

Full docs can be found [here](https://docs.google.com/document/d/15coxZhW00uu35mReiUQC9vUy5uLUeuI1m09Q_P8G4LM/edit?usp=sharing)

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?
This handler integrates the Writer LLM API using langchain, it also supports storing of large contexts in a ChromaDB collection in memory, this will be persisted to parquet in between sessions. It also supports the use of most sentence transformers that are found on Hugging Face Hub

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

This integration is useful for a number of reasons
- Makes it possible to query large document corpus that may exceed most LLM token limit by making use of an intermediate vectorDB, in this case ChromaDB
- You can load in existing persisted ChromaDB with embeddings
- Run Question and Answer queries against the powerful Writer LLM API

An ideal use case would be long and complex business documents that you want to be able to ask questions about or alternatively academic papers or other long form texts of interest

This integration is not suitable for other LLM tasks other than Question Answering

## Are models created with this integration fast and scalable, in general?
The rate limiting step is the ingestion of data from regular Database table in text format to vector embeddings in ChromaDB collection. Depending on the Embedding model used, the size of the document corpus and your compute specification it can take a long time to complete!

Once you have ingested your data, the Writer API is very fast and scalable when querying on a question and answer task

## What are the recommended system specifications for models created with this framework?
Since the Writer LLMs are hosted on by Writer, there is no need for any additional system specifications. As noted above, the embedding step can take longer on less powerful hardware. The only real requirement is to have an internet connection.

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
Users are allowed complete control over the underlying framework by passing parameters via the USING syntax.

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
No, this integration doesn't support DESCRIBE syntax.

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No, fine-tuning is not supported.

## Are there any other noteworthy aspects to this handler?
The handler has a number of default parameters set, the user only needs to pass  `prompt_template`, `writer_api_key` along with `writer_org_id` or alternatively with `base_url`.

The other parameters have default values


## Any directions for future work in subsequent versions of the handler?
tbc

## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
```sql
-- Create Writer engine
CREATE ML_ENGINE writer
FROM writer;

-- Create a Writer model and embed input data
CREATE MODEL writer_demo
FROM mysql_demo_db (select * from demo_fda_context limit 10) --limit to 10 rows for testing purposes
PREDICT answer ---specify any column name that exists inside context table, 'answer' used for illustrative purposes
USING
   engine="writer",
   writer_org_id="",
   writer_api_key="",
   embeddings_model_name="sentence-transformers/all-mpnet-base-v2",
   vector_store_folder_name="writer_demo_vector_store",
   prompt_template="Use the following pieces of context to answer the question at the end. If you do not know the answer,
just say that you do not know, do not try to make up an answer.
Context: {context}
Question: {question}
Helpful Answer:"; --this can be any sentence transformer that is compatible with Hugging Face sentence_transformer library, if none provided defaults to "sentence-transformers/all-mpnet-base-v2"

-- Ask a question on your data using Writer LLM API
SELECT *
FROM writer_demo
WHERE question='what product is best for treating a cold?';
```

--Run evaluation of configured model
```sql
-- Create Writer model
CREATE MODEL writer_demo_evaluate
PREDICT answer
USING
   engine="writer",
     writer_org_id="",
writer_api_key="",
   embeddings_model_name="sentence-transformers/all-mpnet-base-v2",
evaluate_dataset='squad_v2_val_100_sample',
n_rows_evaluation=10,
vector_store_folder_name="writer_demo_eval_vector_store",
prompt_template="Use the following pieces of context to answer the question at the end. If you do not know the answer,
just say that you do not know, do not try to make up an answer.
Context: {context}
Question: {question}
Helpful Answer:";

-- Evaluate model
select * from writer_demo_evaluate where run_evaluation = True;

-- Get evaluation metrics and output from evaluation

NB this will only work if you have run the evaluation query above

DESCRIBE PREDICTOR mindsdb.writer_demo_evaluate.evaluation_output;
DESCRIBE PREDICTOR mindsdb.writer_demo_evaluate.mean_evaluation_metrics;


```
