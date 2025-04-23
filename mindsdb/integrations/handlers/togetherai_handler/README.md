---
title: TogetherAI
sidebarTitle: TogetherAI
---

This documentation describes the integration of MindsDB with [TogetherAI](https://www.together.ai/), a research-driven artificial intelligence company. They provide decentralized cloud services which empower developers and researchers at organizations of all sizes to train, fine-tune, and deploy generative AI models.
This integration allows using TogetherAI models within MindsDB, providing models with access to data from various data sources.

This integration is created by extending [OpenAI Handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/openai_handler)

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. Obtain the TogetherAI API key required to deploy and use TogetherAI models within MindsDB. Follow the [instructions for obtaining the API key](https://docs.together.ai/reference/authentication-1).

## Setup

Create an AI engine from the [TogetherAI handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/togetherai_handler).

```sql
CREATE ML_ENGINE togetherai_engine
FROM togetherai
USING
      togetherai_api_key = 'api-key-value';
```

Create a model using `togetherai_engine` as an engine.

```sql
CREATE MODEL togetherai_model
PREDICT target_column
USING
      engine = 'togetherai_engine',  -- engine name as created via CREATE ML_ENGINE
      mode = 'mode_name', -- optional, mode to run the model in
      model_name = 'meta-llama/Llama-3.2-3B-Instruct-Turbo',  -- optional with default value of meta-llama/Llama-3.2-3B-Instruct-Turbo
      question_column = 'question',  -- optional, column name that stores user input
      context_column = 'context',  -- optional, column that stores context of the user input
      prompt_template = 'input your query here', -- optional, user provides instructions to the model here
      user_column = 'user_input', -- optional, stores user input
      assistant_column = 'conversation_context', -- optional, stores conversation context
      prompt = 'instruction to the model', -- optional stores instruction to the model
      max_tokens = 100, -- optional, token limit for answer
      temperature = 0.3, -- temp
```

The following parameters are available to use when creating model using TogetherAI engine.

* `engine`: This is the engine name as created with the [`CREATE ML_ENGINE`](https://docs.mindsdb.com/mindsdb_sql/sql/create/ml-engine) statement.
* `mode`: This parameter is optional. The available modes include `default`, `conversational`, `conversational-full`, and `embedding`.
  * The `default` mode is used by default. The model replies to the `prompt_template` message.
  * The `conversational` mode enables the model to read and reply to multiple messages.
  * The `conversational-full` mode enables the model to read and reply to multiple messages, one reply per message.
  * The `embedding` mode enables the model to return output in the form of embeddings.
* `model_name`: This parameter is optional. By default, the `meta-llama/Llama-3.2-3B-Instruct-Turbo` model is used.

> You can find [all models supported by each mode here](https://www.together.ai/models).

* `question_column`: This parameter is optional. It contains the column name that stores user input.
* `context_column`: This parameter is optional. It contains the column name that stores context for the user input.
* `prompt_template`: This parameter is optional if you use `question_column`. It stores the message or instructions to the model. *Please note that this parameter can be overridden at prediction time.*
* `max_tokens`: This parameter is optional. It defines the maximum token cost of the prediction. *Please note that this parameter can be overridden at prediction time.*
* `temperature`: This parameter is optional. It defines how *risky* the answers are. The value of `0` marks a well-defined answer, and the value of `0.9` marks a more creative answer. *Please note that this parameter can be overridden at prediction time.*

## Usage

This handler is an extension of the [OpenAI Handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/openai_handler) and its usage is similar, except for the available models.
Check available models: [TogetherAI Models](https://www.together.ai/models)

## Troubleshooting Guide

<Warning>
`Authentication Error`

* **Symptoms**: Failure to authenticate to the TogetherAI API.
* **Checklist**:
    1. Make sure that your TogetherAI account is active.
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
            JOIN togetherai_model AS output
            ```
        * Incorrect: 
            ```sql
            SELECT input.text, output.sentiment
            FROM integration.'travel data' AS input
            JOIN togetherai_model AS output
            ```
        * Correct:  
            ```sql 
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN togetherai_model AS output
            ```
</Warning>
