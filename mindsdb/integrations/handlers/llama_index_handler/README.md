## LlamaIndex Handler
LlamaIndex ML handler for MindsDB, create indexes over data plugged in mindsdb and use it to create a Question & Answer (Q&A) system

## LlamaIndex
LlamaIndex is a data framework for your LLM application. In this handler, we use the LlamaIndex package, which is available in Python. More information about this Python client can be found (here)[https://gpt-index.readthedocs.io/en/latest/].

## Implemented Features
- [x] LlamaIndex ML Handler
  - [x] [Support Web Page Reader](https://gpt-index.readthedocs.io/en/latest/examples/data_connectors/WebPageDemo.html)
  - [x] [Support Database Reader](https://gpt-index.readthedocs.io/en/latest/examples/data_connectors/DatabaseReaderDemo.html)
  - [x] [Support Github Reader](https://llamahub.ai/l/youtube_transcript?from=loaders)
  - [x] [Support YoutubeTranscript Reader](https://llamahub.ai/l/youtube_transcript?from=loaders) **Note: To run YoutubeLoader `pip install youtube_transcript_api`**


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

![](https://i.ibb.co/WPgXJDs/Screenshot-2023-05-30-at-7-54-32-PM.png)

## LLM conversational mode
```sql
  CREATE MODEL chatbot_agent_2
    PREDICT answer
    USING
    engine = 'llama_index',
    input_column = 'question',
    openai_api_key = '<key>',
    mode = 'conversational',
    user_column = 'question' ,
    assistant_column = 'answer'
```


## Example usage for GithubLoader
```sql
CREATE MODEL github_loader
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'VectorStoreIndex',
  owner = 'mindsdb',
  repo = 'mindsdb',
  branch = 'staging',
  reader = 'GithubRepositoryReader',
  filter_type = 'include',
  filter_file_extensions = ['.py','.html','.md'],
  input_column = 'questions',
  openai_api_key = '<your_openai_key>',
  github_token = '<your_github_token>';
```

```sql
SELECT a.questions, b.answer
FROM github_loader as b
JOIN files.questions as a
```

```sql
SELECT question, answer
FROM github_loader
WHERE questions = 'Explain steps to setup MindsDB on local machine?'
```

## Example usage for YoutubeTranscriptLoader
```sql
CREATE MODEL youtube_loader
PREDICT answer
USING 
  engine = 'llama_index', 
  index_class = 'VectorStoreIndex',
  ytlinks = ['<link_of_youtube_videos>'],
  reader = 'YoutubeTranscriptReader',
  input_column = 'questions',
  openai_api_key = '<your_openai_key>';
```

```sql
SELECT question, answer
FROM youtube_loader
WHERE questions = 'What was the video about?'
```