---
title: Groq
sidebarTitle: Groq
---

This documentation describes the integration of MindsDB with [Groq](https://groq.com/), a cloud service that simplifies the way developers interact with cutting-edge LLMs through its API.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Groq within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Groq API key required to deploy and use Groq models within MindsDB. Get the API key from [here](https://console.groq.com/keys).

## Setup

Create an AI engine from the [Groq handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/groq_handler).

```sql
CREATE ML_ENGINE groq_engine
FROM groq
USING
      groq_api_key = 'api-key-value'
```

Create a model using `groq_engine` as an engine.

```sql
CREATE MODEL groq_model
PREDICT answer
USING
      engine = 'groq_engine',   -- engine name as created via CREATE ML_ENGINE
      model_name = 'model-name',              -- choose one of available models
      prompt_template = 'prompt-to-the-model' -- prompt message to be completed by the model
      question_column = 'question',  -- optional, column name that stores user input
      context_column = 'context',  -- optional, column that stores context of the user input
      prompt_template = 'input your query here', -- optional, user provides instructions to the model here
      user_column = 'user_input', -- optional, stores user input
      assistant_column = 'conversation_context', -- optional, stores conversation context
      prompt = 'instruction to the model', -- optional stores instruction to the model
      max_tokens = 100, -- optional, token limit for answer
      temperature = 0.3, -- temp
```

## Usage

The following usage examples utilize `groq_engine` to create a model with the `CREATE MODEL` statement.

Classify text sentiment using the Mistral 7B model.

```sql
CREATE MODEL groq_model
PREDICT sentiment
USING
   engine = 'groq_engine',
   model_name = 'llama3-8b-8192',
   prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}. Give sentiment as result only.';
```

Query the model to get predictions.

```sql
SELECT text, sentiment
FROM groq_model
WHERE text = 'I love machine learning!';
```

Here is the output:

```sql
+--------------------------+-----------+
| text                     | sentiment |
+--------------------------+-----------+
| I love machine learning! | positive  |
+--------------------------+-----------+
```

## Supported Models

For an overview of the models supported, visit the [following docs](https://console.groq.com/docs/models). This list will help you quickly identify the right models for your needs.

## Troubleshooting Guide

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    Examples:
        * Incorrect:

            ```sql
            SELECT input.text, output.sentiment
            FROM integration.travel data AS input
            JOIN groq_model AS output
            ```

        * Incorrect:

            ```sql
            SELECT input.text, output.sentiment
            FROM integration.'travel data' AS input
            JOIN groq_model AS output
            ```

        * Correct:  

            ```sql
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN groq_model AS output
            ```

</Warning>
