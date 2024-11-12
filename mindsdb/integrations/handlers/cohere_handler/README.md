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
      task = 'task_name',        -- choose one of 'text-summarization', 'text-generation'
      column = 'column_name';    -- column that stores input/question to the model
```

## Usage

The following usage examples utilize `cohere_engine` to create a model with the `CREATE MODEL` statement.

Create a model to predict the answer to a question using the `text-generation` task.

```sql
CREATE MODEL cohere_model
PREDICT answer
USING
      engine = 'cohere_engine',
      task = 'text-generation',
      column = 'question';
```

Where:

| Name              | Description                                                            |
|-------------------|------------------------------------------------------------------------|
| `task`            | It defines the task to be accomplished.                                |
| `column`          | It defines the column with the text to be acted upon.                  |
| `engine`          | It defines the Cohere engine.                                          |

Query the model to get predictions.

```sql
SELECT answer
FROM cohere_model
WHERE question = 'What is the capital of France?';
```

Here is the output:

| answer |
| ------ |
| The capital of France is Paris. Paris is France's largest city and a major global center for art, culture, fashion, and cuisine. It is renowned for its iconic landmarks such as the Eiffel Tower, Notre-Dame Cathedral, and the Louvre Museum.

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>
