## LlamaIndex Github Handler
LlamaIndex Github ML handler for MindsDB, create indexes over data plugged in mindsdb from GitHub Repositories and use it to create a Question & Answer (Q&A) system

## LlamaIndex
LlamaIndex is a data framework for your LLM application. In this handler, we use the LlamaIndex package, which is available in Python. More information about this Python client can be found [here](https://gpt-index.readthedocs.io/en/latest/).

## Implemented features
- [x] [Create a LlamaIndex index over a GitHub repository](https://llamahub.ai/l/github_repo)

## How to use
In the below example, we will only ingest the .md and .py files in datasource. We can also filter directories with **filter_directories**

```sql
    CREATE MODEL github_repo
    PREDICTING answer
    USING
       engine='llama_index_github',
       index_class='VectorStoreIndex',
       owner='mindsdb',
       repo='mindsdb',
       filter_type='include',
       filter_file_extensions=['.md', '.py'],
       input_column='question',
       github_token='your_github_token',
       open_api_key='your_open_ai_key',
```

```sql
SELECT question,answer
FROM mindsdb.github_repo
WHERE question = 'Explain all classes in mindsdb';
```

```sql
SELECT a.question,b.answer
FROM mindsdb.github_repo as b
JOIN files.questions as a
```

![](https://imgur.com/a/cDkInA3)


