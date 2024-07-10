---
title: Cohere
sidebarTitle: Cohere
---

This documentation describes the integration of MindsDB with [Cohere](https://cohere.com/), a technology company focused on artificial intelligence for the enterprise.
The integration allows for the deployment of Cohere models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Cohere within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Cohere API key required to deploy and use Cohere models within MindsDB. Sign up for a Cohere account and request an API key from the Cohere dashboard. Learn more [here](https://cohere.com/pricing).

## Setup

Create an AI engine from the [Cohere handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/cohere_handler).

```sql
CREATE ML_ENGINE cohere_engine
FROM cohere
USING
    cohere_api_key = 'your-cohere-api-key';
```

Create a model using `cohere_engine` as an engine.

```sql
CREATE MODEL cohere_model
PREDICT target_column
USING
      engine = 'cohere_engine',  -- engine name as created via CREATE ML_ENGINE
      task = 'task_name',        -- choose one of 'language-detection', 'text-summarization', 'text-generation'
      column = 'column_name';    -- column that stores input/question to the model
```

## Usage

The following usage examples utilize `cohere_engine` to create a model with the `CREATE MODEL` statement.

Create a model to detect language of a given input text.

```sql
CREATE MODEL cohere_model
PREDICT language
USING
      engine = 'cohere_engine',
      task = 'language-detection',
      column = 'text';
```

Where:

| Name              | Description                                                            |
|-------------------|------------------------------------------------------------------------|
| `task`            | It defines the task to be accomplished.                                |
| `column`          | It defines the column with the text to be acted upon.                  |
| `engine`          | It defines the Cohere engine.                                          |

Query the model to get predictions.

```sql
SELECT text, language
FROM cohere_model
WHERE text = '¿Cómo estás?';
```

Here is the output:

```sql
+--------------+----------+
| text         | language |
+--------------+----------+
| ¿Cómo estás? | Spanish  |
+--------------+----------+
```

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>
