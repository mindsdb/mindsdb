## LlamaIndex Handler
LlamaIndex ML handler for MindsDB, create indexes over data plugged in mindsdb and use it to create a Question & Answer (Q&A) systems

## LlamaIndex
LlamaIndex is a data framework for your LLM application.In this handler,python client of LlamaIndex api is used and more information about this python client can be found (here)[https://gpt-index.readthedocs.io/en/latest/]

## Implemented Features
- [x] LlamaIndex ML Handler
  - [x] [Support Web Page Reader](https://gpt-index.readthedocs.io/en/latest/examples/data_connectors/WebPageDemo.html)



## Example Usage

~~~sql
CREATE MODEL my_qa_model1
FROM files
    (SELECT * FROM about_mindsdb)
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
  query_engine = 'as_query_engine', 
  reader = 'DFReader',
  input_column = 'question',
  openai_api_key = '{your_open_api_key}';
~~~~
~~~sql
SELECT question,answer
FROM mindsdb.my_qa_model1
WHERE question = 'What problem does MindsDB solves?';
~~~~

~~~sql
CREATE MODEL my_qa_model2
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
  query_engine = 'as_query_engine', 
  reader = 'SimpleWebPageReader',
  source_url_link = 'https://mindsdb.com/about',
  input_column = 'question',
  openai_api_key = '{your_open_api_key}'
~~~~

~~~sql
SELECT question,answer
FROM mindsdb.my_qa_model2
WHERE question = 'Who are the community maintainers for MindsDB?';
~~~~

~~~sql
SELECT t.question, m.answer
FROM mindsdb.my_qa_model2 as m
JOIN files.question_table as t;
~~~~

![](https://i.ibb.co/WPgXJDs/Screenshot-2023-05-30-at-7-54-32-PM.png)
