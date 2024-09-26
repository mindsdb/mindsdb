---
title: OpenAI
sidebarTitle: OpenAI
---

This documentation describes the integration of MindsDB with [OpenAI](https://openai.com/), an AI research organization known for developing AI models like GPT-3 and GPT-4.
The integration allows for the deployment of OpenAI models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use OpenAI within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the OpenAI API key required to deploy and use OpenAI models within MindsDB. Follow the [instructions for obtaining the API key](https://help.openai.com/en/articles/4936850-where-do-i-find-my-secret-api-key).

## Setup

Create an AI engine from the [OpenAI handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/openai_handler).

```sql
CREATE ML_ENGINE openai_engine
FROM openai
USING
      openai_api_key = 'api-key-value';
```

Create a model using `openai_engine` as an engine.

```sql
CREATE MODEL openai_model
PREDICT target_column
USING
      engine = 'openai_engine',  -- engine name as created via CREATE ML_ENGINE
      api_base = 'base-url', -- optional, replaces the default base URL
      mode = 'mode_name', -- optional, mode to run the model in
      model_name = 'openai_model_name',  -- optional with default value of gpt-3.5-turbo
      question_column = 'question',  -- optional, column name that stores user input
      context_column = 'context',  -- optional, column that stores context of the user input
      prompt_template = 'input your query here', -- optional, user provides instructions to the model here
      user_column = 'user_input', -- optional, stores user input
      assistant_column = 'conversation_context', -- optional, stores conversation context
      prompt = 'instruction to the model', -- optional stores instruction to the model
      max_tokens = 100, -- optional, token limit for answer
      temperature = 0.3, -- temp

```

The following parameters are available to use when creating an OpenAI model:

* `engine`: This is the engine name as created with the [`CREATE ML_ENGINE`](https://docs.mindsdb.com/mindsdb_sql/sql/create/ml-engine) statement.
* `api_base`: This parameter is optional. It replaces the default OpenAI's base URL with the defined value.
* `mode`: This parameter is optional. The available modes include `default`, `conversational`, `conversational-full`, `image`, and `embedding`.
    - The `default` mode is used by default. The model replies to the `prompt_template` message.
    - The `conversational` mode enables the model to read and reply to multiple messages.
    - The `conversational-full` mode enables the model to read and reply to multiple messages, one reply per message.
    - The `image` mode is used to create an image instead of a text reply.
    - The `embedding` mode enables the model to return output in the form of embeddings.
> You can find [all models supported by each mode here](https://github.com/mindsdb/mindsdb/blob/main/mindsdb/integrations/handlers/openai_handler/constants.py).

* `model_name`: This parameter is optional. By default, the `gpt-3.5-turbo` model is used.
> You can find [all available models here](https://github.com/mindsdb/mindsdb/blob/main/mindsdb/integrations/handlers/openai_handler/constants.py).

* `question_column`: This parameter is optional. It contains the column name that stores user input.
* `context_column`: This parameter is optional. It contains the column name that stores context for the user input.
* `prompt_template`: This parameter is optional if you use `question_column`. It stores the message or instructions to the model. *Please note that this parameter can be overridden at prediction time.*
* `max_tokens`: This parameter is optional. It defines the maximum token cost of the prediction. *Please note that this parameter can be overridden at prediction time.*
* `temperature`: This parameter is optional. It defines how *risky* the answers are. The value of `0` marks a well-defined answer, and the value of `0.9` marks a more creative answer. *Please note that this parameter can be overridden at prediction time.*

## Usage

Here are the combination of parameters for creating a model:

1. Provide a `prompt_template` alone.
2. Provide a `question_column` and optionally a `context_column`.
3. Provide a `prompt`, `user_column`, and `assistant_column` to create a model in the conversational mode.

The following usage examples utilize `openai_engine` to create a model with the `CREATE MODEL` statement.

### Answering questions without context

Here is how to create a model that answers questions without context.

```sql
CREATE MODEL openai_model
PREDICT answer
USING
    engine = 'openai_engine',
    question_column = 'question';
```

Query the model to get predictions.

```sql
SELECT question, answer
FROM openai_model
WHERE question = 'Where is Stockholm located?';
```

Here is the output:

```sql
+---------------------------+-------------------------------+
|question                   |answer                         |
+---------------------------+-------------------------------+
|Where is Stockholm located?|Stockholm is located in Sweden.|
+---------------------------+-------------------------------+
```

### Answering questions with context

```sql
CREATE MODEL openai_model
PREDICT answer
USING
    engine = 'openai_engine',
    question_column = 'question',
    context_column = 'context';
```

Query the model to get predictions.

```sql
SELECT context, question, answer
FROM openai_model
WHERE context = 'Answer accurately'
AND question = 'How many planets exist in the solar system?';
```

On execution, we get:

```sql
+-------------------+-------------------------------------------+----------------------------------------------+
|context            |question                                   |answer                                        |
+-------------------+-------------------------------------------+----------------------------------------------+
|Answer accurately  |How many planets exist in the solar system?| There are eight planets in the solar system. |
+-------------------+-------------------------------------------+----------------------------------------------+
```

### Prompt completion

Here is how to create a model that offers the most flexible mode of operation. It answers any query provided in the `prompt_template` parameter.

<Tip>
Good prompts are the key to getting great completions out of large language models like the ones that OpenAI offers. For best performance, we recommend you read their [prompting guide](https://beta.openai.com/docs/guides/completion/prompt-design) before trying your hand at prompt templating.
</Tip>

Let's look at an example that reuses the `openai_model` model created earlier and overrides parameters at prediction time.

```sql
SELECT instruction, answer
FROM openai_model
WHERE instruction = 'Speculate extensively'
USING
    prompt_template = '{{instruction}}. What does Tom Hanks like?',
    max_tokens = 100,
    temperature = 0.5;
```

On execution, we get:

```sql
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|instruction           |answer                                                                                                                                                                                                                         |
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Speculate extensively |Some people speculate that Tom Hanks likes to play golf, while others believe that he enjoys acting and directing. It is also speculated that he likes to spend time with his family and friends, and that he enjoys traveling.|
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Conversational mode

Here is how to create a model in the conversational mode.

```sql
CREATE MODEL openai_chat_model
PREDICT response
USING
  engine = 'openai_engine',
  mode = 'conversational',
  model_name = 'gpt-3.5-turbo',
  user_column = 'user_input',
  assistant_column = 'conversation_history',
  prompt = 'Answer the question in a helpful way.';
```

And here is how to query this model:

```sql
SELECT response
FROM openai_chat_model
WHERE user_input = '<question>'
AND conversation_history = '<optionally, provide the context for the question>';
```

## Next Steps

Follow [this tutorial on sentiment analysis](https://docs.mindsdb.com/use-cases/data_enrichment/sentiment-analysis-inside-mysql-with-openai) and [this tutorial on finetuning OpenAI models](https://docs.mindsdb.com/use-cases/automated_finetuning/openai) to see more use case examples.

## Troubleshooting Guide

<Warning>
`Authentication Error`

* **Symptoms**: Failure to authenticate to the OpenAI API.
* **Checklist**:
    1. Make sure that your OpenAI account is active.
    2. Confirm that your API key is correct.
    3. Ensure that your API key has not been revoked.
    4. Ensure that you have not exceeded the API usage or rate limit.
</Warning>

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table and model names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    Examples:
        * Incorrect:
            ```sql
            SELECT input.text, output.sentiment
            FROM integration.travel data AS input
            JOIN openai_engine AS output
            ```
        * Incorrect: 
            ```sql
            SELECT input.text, output.sentiment
            FROM integration.'travel data' AS input
            JOIN openai_engine AS output
            ```
        * Correct:  
            ```sql 
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN openai_engine AS output
            ```
</Warning>
