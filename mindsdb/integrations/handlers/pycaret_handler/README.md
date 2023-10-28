## PyCaret Handler

PyCaret ML handler for MindsDB.

## PyCaret

PyCaret is an open-source, low-code machine learning library in Python that automates machine learning workflows.














## Example Usage

~~~sql
CREATE MODEL my_qa_model1
FROM files
    (SELECT * FROM about_mindsdb)
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'GPTVectorStoreIndex',
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
