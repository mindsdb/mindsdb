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
The handler has a number of default parameters set, the user only needs to pass  `vector_store_folder_name`, `writer_api_key` along with `writer_org_id` or alternatively with `base_url`.

The other parameters have default values

Supported query parameters for USING syntax are as follows:

## For the Writer LLM API

- `prompt_template` - this is the template that is used to generate the prompt. If not provided, the default is `DEFAULT_QA_PROMPT_TEMPLATE` (see `settings.py` in `rag_handler` for more details)
- `writer_api_key` - this is the API key that is used to authenticate with the Writer LLM API
- `writer_org_id` - this is the organization ID that is used to authenticate with the Writer LLM API
- `base_url` - this is the base URL that is used to authenticate with the Writer LLM API, optional, if not provided uses org_id and model_id
- `model_id` - this is the model ID that is used to authenticate with the Writer LLM API
- `max_tokens` - this is the maximum number of tokens that generated output will be, the default is 1024
- `temperature` - this is the temperature that is used to generate the output, the default is 0.0
- `top_p` - this is the top p that is used to generate the output, the default is 1
- `stop` - Sequences when completion generation will stop, the default is an empty list
- `best_of` - this is the number of best of that are used to generate the output, the default is 5
- `verbose` - this is a boolean that determines if the output is verbose, the default is False

## For the Vector Store and Embeddings Model

- `chunk_size` - this is the number of rows that are ingested at a time, the default is 500
- `chunk_overlap` - this is the number of rows that are overlapped between chunks, the default is 50
- `generation_evaluation_metrics` - this is a list of metrics that are used to evaluate the generation task, the default is all of the available metrics
- `retrieval_evaluation_metrics` - this is a list of metrics that are used to evaluate the retrieval task, the default is all of the available metrics
- `evaluation_type` - this is the type of evaluation that is run, the default is e2e, this can be set to either `generation` or `retrieval`
- `n_rows_evaluation` - this is the number of rows that are used to evaluate the model, the default is None, which means all rows are used
- `retriever_match_threshold` - this is the threshold that is used to determine if the retriever has found a match, the default is 0.7
- `generator_match_threshold` - this is the threshold that is used to determine if the generator has found a match, the default is 0.8
- `evaluate_dataset` - this is the dataset that is used to evaluate the model, the default is 'squad_v2_val_100_sample'
- `run_embeddings` - this is a boolean that determines if the embeddings are run, the default is True
- `top_k` - this is the number of results that are returned from the retriever, the default is 4
- `embeddings_model_name` - this is the name of the sentence transformer model that is used to generate the embeddings, the default is `BAAI/bge-base-en`
- `context_columns` - this is a list of columns that are used to generate the context, the default is None, which means all columns are used
- `vector_store_name` - this is the name of the vector store that is used to store the embeddings, the default is `chroma`
- `collection_name` - this is the name of the collection that is used to store the embeddings, the default is `collection`
- `summarize_context` - this is a boolean that determines if the context is summarized, the default is True
- `summarization_prompt_template` - this is the template that is used to summarize the context, the default is `SUMMARIZATION_PROMPT_TEMPLATE` (see settings.py in rag_handler for more details)
- `vector_store_folder_name` - this is the name of the folder that is used to store the vector store - it must be specified by user

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
   vector_store_folder_name="writer_demo_vector_store";

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
    evaluate_dataset='squad_v2_val_100_sample',
    n_rows_evaluation=10,
    vector_store_folder_name="writer_demo_eval_vector_store";

-- Evaluate model
select * from writer_demo_evaluate where run_evaluation = True;

-- Get evaluation metrics and output from evaluation

--NB this will only work if you have run the evaluation query above

DESCRIBE PREDICTOR mindsdb.writer_demo_evaluate.evaluation_output;
DESCRIBE PREDICTOR mindsdb.writer_demo_evaluate.mean_evaluation_metrics;


```
