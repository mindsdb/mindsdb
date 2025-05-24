---
title: Linkup
sidebarTitle: Linkup
---

## Linkup Handler

This documentation describes the integration of MindsDB with [Linkup](https://docs.linkup.so/pages/documentation/get-started/introduction), a framework for building context-augmented generative AI applications with LLMs. 


## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Linkup within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Linkup API key. Go to [Linkup](https://www.linkup.so).

## Setup

Create an AI engine from the [Linkup handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/linkup_handler).

```sql
CREATE ML_ENGINE linkup
FROM linkup;
```

## Usage

Here is how to create a model that answers questions by reading a page from the web:

```sql
CREATE MODEL linkup_model
PREDICT answer
USING 
  engine = 'linkup', 
  api_key = 'your_linkup_api_key',
  depth = 'deep',                   -- or standard
  output_type = 'sourcedAnswer',    -- or searchResults
  input_column = 'question';
```

Query the model to get answer:

```sql
SELECT question, answer
FROM mindsdb.linkup_model
WHERE question = "What is MindsDB's story?"
```

Here is the output:

```sql
+---------------------------+-------------------------------+
|question                   |answer                         |
+---------------------------+-------------------------------+
|What is MindsDB's story?    |MindsDB was founded in December
                                2017 by Jorge Torres and Adam Carrigan,
                                initially as an open-source project...|
+---------------------------+-------------------------------+

```


