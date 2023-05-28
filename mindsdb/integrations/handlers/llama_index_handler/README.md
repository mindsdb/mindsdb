## LlamaIndex Handler
LlamaIndex ML handler for MindsDB, create indexes over data plugged in mindsdb and use it to create a Question & Answer (Q&A) systems

## LlamaIndex
LlamaIndex is a data framework for your LLM application.In this handler,python client of LlamaIndex api is used and more information about this python client can be found (here)[https://gpt-index.readthedocs.io/en/latest/]

## Implemented Features
- [x] LlamaIndex ML Handler
  - [x] [Support Web Page Reader](https://gpt-index.readthedocs.io/en/latest/examples/data_connectors/WebPageDemo.html)

## Example Usage
~~~sql
CREATE MODEL my_qa_model
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
  query_engine = 'as_query_engine', 
  openai_api_key = '{your_openai_api_key}',
  source_url_link = '{your_source_url_link}';
~~~~

~~~sql
SELECT question, answer
FROM mindsdb.my_qa_model
WHERE question = 'Who founded MindsDB?";
~~~~
