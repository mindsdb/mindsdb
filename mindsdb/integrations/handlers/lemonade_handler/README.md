---
title: Lemonade
sidebarTitle: Lemonade
---

This documentation describes the integration of MindsDB with [Lemonade](https://github.com/lemonade-sdk/lemonade), a local LLM server that provides OpenAI-compatible API endpoints for running large language models locally with GPU and NPU acceleration.

The integration allows for the deployment of local LLM models within MindsDB, providing the models with access to data from various data sources while running entirely on your local hardware.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Lemonade within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Install and run the Lemonade server locally. Follow the [Lemonade installation instructions](https://github.com/lemonade-sdk/lemonade#getting-started).
4. The Lemonade server should be running on `http://localhost:8000` (default) or your custom endpoint.

## Setup

Create an AI engine from the [Lemonade handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/lemonade_handler).

```sql
CREATE ML_ENGINE lemonade_engine
FROM lemonade
USING
      lemonade_api_key = 'lemonade';  -- Can be any value, required but unused
```

Create a model using `lemonade_engine` as an engine.

```sql
CREATE MODEL lemonade_model
PREDICT target_column
USING
      engine = 'lemonade_engine',  -- engine name as created via CREATE ML_ENGINE
      api_base = 'http://localhost:8000/api/v1', -- optional, replaces the default base URL
      mode = 'mode_name', -- optional, mode to run the model in
      model_name = 'Llama-3.2-1B-Instruct-Hybrid',  -- optional with default value
      question_column = 'question',  -- optional, column name that stores user input
      context_column = 'context',  -- optional, column that stores context of the user input
      prompt_template = 'input your query here', -- optional, user provides instructions to the model here
      user_column = 'user_input', -- optional, stores user input
      assistant_column = 'conversation_context', -- optional, stores conversation context
      prompt = 'instruction to the model', -- optional stores instruction to the model
      max_tokens = 100, -- optional, token limit for answer
      temperature = 0.3, -- temp
```

The following parameters are available to use when creating a Lemonade model:

* `engine`: This is the engine name as created with the [`CREATE ML_ENGINE`](https://docs.mindsdb.com/mindsdb_sql/sql/create/ml-engine) statement.
* `api_base`: This parameter is optional. It replaces the default Lemonade base URL with the defined value.
* `mode`: This parameter is optional. The available modes include `default`, `conversational`, and `conversational-full`.
    - The `default` mode is used by default. The model replies to the `prompt_template` message.
    - The `conversational` mode enables the model to read and reply to multiple messages.
    - The `conversational-full` mode enables the model to read and reply to multiple messages, one reply per message.
> Note: Lemonade does not support image generation or embeddings like OpenAI.

* `model_name`: This parameter is optional. By default, the `Llama-3.2-1B-Instruct-Hybrid` model is used.
> You can find [all available models here](https://github.com/lemonade-sdk/lemonade#model-library).

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

The following usage examples utilize `lemonade_engine` to create a model with the `CREATE MODEL` statement.

### Answering questions without context

Here is how to create a model that answers questions without context.

```sql
CREATE MODEL lemonade_model
PREDICT answer
USING
    engine = 'lemonade_engine',
    question_column = 'question';
```

Query the model to get predictions.

```sql
SELECT question, answer
FROM lemonade_model
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
CREATE MODEL lemonade_model
PREDICT answer
USING
    engine = 'lemonade_engine',
    question_column = 'question',
    context_column = 'context';
```

Query the model to get predictions.

```sql
SELECT context, question, answer
FROM lemonade_model
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
Good prompts are the key to getting great completions out of large language models like the ones that Lemonade offers. For best performance, we recommend you read the [Lemonade documentation](https://github.com/lemonade-sdk/lemonade) for more information about supported models and configurations.
</Tip>

Let's look at an example that reuses the `lemonade_model` model created earlier and overrides parameters at prediction time.

```sql
SELECT instruction, answer
FROM lemonade_model
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
CREATE MODEL lemonade_chat_model
PREDICT response
USING
  engine = 'lemonade_engine',
  mode = 'conversational',
  model_name = 'Llama-3.2-1B-Instruct-Hybrid',
  user_column = 'user_input',
  assistant_column = 'conversation_history',
  prompt = 'Answer the question in a helpful way.';
```

And here is how to query this model:

```sql
SELECT response
FROM lemonade_chat_model
WHERE user_input = '<question>'
AND conversation_history = '<optionally, provide the context for the question>';
```

## Supported Models

Lemonade supports various local LLM models including:

- **Llama models**: Llama-3.2-1B-Instruct-Hybrid, Llama-3.2-3B-Instruct-Hybrid, etc.
- **Gemma models**: Gemma-3-4b-it-GGUF, Gemma-3-8b-it-GGUF
- **Mistral models**: Mistral-7B-Instruct-v0.3
- **Qwen models**: Qwen2.5-7B-Instruct
- **Phi models**: Phi-3.5-mini-instruct
- And many more...

For a complete list of supported models, see the [Lemonade Model Library](https://github.com/lemonade-sdk/lemonade#model-library).

## Hardware Support

Lemonade supports various hardware configurations:

- **CPU**: All platforms
- **GPU**: Vulkan (all platforms), ROCm (selected AMD platforms), Metal (Apple Silicon)
- **NPU**: AMD Ryzen™ AI 300 series (Windows only)

For more information about hardware support, see the [Lemonade documentation](https://github.com/lemonade-sdk/lemonade#supported-configurations).

## Next Steps

Follow [this tutorial on sentiment analysis](https://docs.mindsdb.com/use-cases/data_enrichment/sentiment-analysis-inside-mysql-with-openai) to see more use case examples with local LLM models.

## Troubleshooting Guide

<Warning>
`Connection Error`

* **Symptoms**: Failure to connect to the Lemonade server.
* **Checklist**:
    1. Make sure that the Lemonade server is running on the expected port (default: 8000).
    2. Confirm that the `api_base` URL is correct.
    3. Ensure that the Lemonade server is accessible from MindsDB.
    4. Check the Lemonade server logs for any errors.
</Warning>

<Warning>
`Model Not Found Error`

* **Symptoms**: The specified model is not available on the Lemonade server.
* **Checklist**:
    1. Ensure that the model is installed on the Lemonade server.
    2. Use the `lemonade-server list` command to see available models.
    3. Install the required model using `lemonade-server pull <model_name>`.
    4. Verify that the model name is correct (case-sensitive).
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
            JOIN lemonade_engine AS output
            ```
        * Incorrect: 
            ```sql
            SELECT input.text, output.sentiment
            FROM integration.'travel data' AS input
            JOIN lemonade_engine AS output
            ```
        * Correct:  
            ```sql 
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN lemonade_engine AS output
            ```
</Warning>
