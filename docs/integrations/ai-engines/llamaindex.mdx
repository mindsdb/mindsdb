---
title: LlamaIndex
sidebarTitle: LlamaIndex
---

## LlamaIndex Handler

This documentation describes the integration of MindsDB with [LlamaIndex](https://docs.llamaindex.ai/en/stable/), a framework for building context-augmented generative AI applications with LLMs. 


## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use LlamaIndex within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the OpenAI API key required to OpenAI LLMs. Follow the [instructions for obtaining the API key](https://help.openai.com/en/articles/4936850-where-do-i-find-my-secret-api-key).

## Setup

Create an AI engine from the [Llamaindex handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/llama_index_handler).

```sql
CREATE ML_ENGINE llama_index
FROM llama_index
USING
      openai_api_key = 'api-key-value';
```

Create a model using `llama_index` as an engine and  OpenAI as a model provider.

```sql
CREATE MODEL chatbot_model
PREDICT answer
USING
  engine = 'llama_index',  -- engine name as created via CREATE ML_ENGINE
  input_column = 'question',
  mode = 'conversational', -- optional
  user_column = 'question', -- optional: used only for conversational mode
  assistant_column = 'answer'; -- optional: used only for conversational mode
```


## Usage

Here is how to create a model that answers questions by reading a page from the web:

```sql
CREATE MODEL qa_model
PREDICT answer
USING 
  engine = 'llama_index', 
  reader = 'SimpleWebPageReader',
  source_url_link = 'https://mindsdb.com/about',
  input_column = 'question';
```

Query the model to get answer:

```sql
SELECT question, answer
FROM mindsdb.qa_model
WHERE question = "What is MindsDB's story?"
```

Here is the output:

```sql
+---------------------------+-------------------------------+
|question                   |answer                         |
+---------------------------+-------------------------------+
|What is MindsDB's story?    |MindsDB is a fast-growing open-source ...|
+---------------------------+-------------------------------+

```

### Configuring SimpleWebPageReader for Specific Domains

When SimpleWebPageReader is used it can be configured to interact only with specific domains by using the `web_crawling_allowed_sites` setting in the `config.json` file. 
This feature allows you to restrict the handler to read and process content only from the domains you specify, enhancing security and control over web interactions.

To configure this, simply list the allowed domains under the `web_crawling_allowed_sites` key in `config.json`. For example:

```json
"web_crawling_allowed_sites": [
    "https://docs.mindsdb.com",
    "https://another-allowed-site.com"
]
```

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>

