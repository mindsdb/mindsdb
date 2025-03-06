---
title: DeepSeek
sidebarTitle: DeepSeek
---

This documentation describes the integration of MindsDB with [DeepSeek](https://www.deepseek.com/), an AI research organization known for developing AI models like DeepSeek V3 and DeepSeek R1.
The integration allows for the deployment of DeepSeek models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use DeepSeek within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the DeepSeek API key required to deploy and use DeepSeek models within MindsDB. You can get the API key by signing up on the [DeepSeek Platform](https://platform.deepseek.com/api_keys).

## Setup

Create an AI engine from the [DeepSeek handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/deepseek_handler).

```sql
CREATE ML_ENGINE deepseek_engine
FROM deepseek
USING
    api_base = 'base-url',  -- optional, replaces the default base URL
    deepseek_api_key = 'api-key-value'; -- DeepSeek API key
```

Create a model using `deepseek_engine` as an engine.

```sql
CREATE MODEL deepseek_model
PREDICT target_column
USING
      engine = 'deepseek_engine',  -- engine name as created via CREATE ML_ENGINE
      api_base = 'base-url', -- optional, replaces the default base URL
      model_name = 'deepseek_model_name',  -- optional with default value of `deepseek-chat`
      question_column = 'question',  -- optional, column name that stores user input
      context_column = 'context',  -- optional, column that stores context of the user input
      prompt_template = 'input your query here', -- optional, user provides instructions to the model here
      user_column = 'user_input', -- optional, stores user input
      prompt = 'instruction to the model'; -- optional stores instruction to the model

```

The following parameters are available to use when creating an DeepSeek model:

* `engine`: This is the engine name as created with the [`CREATE ML_ENGINE`](https://docs.mindsdb.com/mindsdb_sql/sql/create/ml-engine) statement.
* `api_base`: This parameter is optional. It replaces the default DeepSeek's base URL with the defined value.
* `model_name`: This parameter is optional. By default, the `deepseek-chat` model is used.
* `question_column`: This parameter is optional. It contains the column name that stores user input.
* `context_column`: This parameter is optional. It contains the column name that stores context for the user input.
* `prompt_template`: This parameter is optional if you use `question_column`. It stores the message or instructions to the model. *Please note that this parameter can be overridden at prediction time.*

## Usage

The following usage examples utilize `deepseek_engine` to create a model with the `CREATE MODEL` statement.

### Answering questions without context

Here is how to create a model that answers questions without context.

```sql
CREATE MODEL deepseek_model
PREDICT answer
USING
    engine = 'deepseek_engine',
    question_column = 'question';
```

Query the model to get predictions.

```sql
SELECT question, answer
FROM deepseek_model
WHERE question = 'Where is Stockholm located?';
```

Here is the output:

| question | answer |
| -------- | ------ |
| Where is Stockholm located? | Stockholm is the capital city of Sweden, located in the eastern part of the country. It is situated on the southeastern coast at the junction of Lake Mälaren and the Baltic Sea. The city is spread across 14 islands that are part of the Stockholm archipelago, which includes over 30,000 islands, islets, and rocks. Stockholm is known for its beautiful waterways, historic architecture, and vibrant cultural scene. It serves as the political, economic, and cultural center of Sweden. |

### Answering questions with context

```sql
CREATE MODEL deepseek_model
PREDICT answer
USING
    engine = 'deepseek_engine',
    question_column = 'question',
    context_column = 'context';
```

Query the model to get predictions.

```sql
SELECT context, question, answer
FROM deepseek_model
WHERE context = 'Answer accurately'
AND question = 'How many planets exist in the solar system?';
```

On execution, we get:

| context | question | answer |
| ------- | -------- | ------ |
| Answer accurately | How many planets exist in the solar system? | 8 |

### JSON-Struct Mode

```sql
CREATE MODEL product_extract_json
PREDICT json
USING
    engine = 'deepseek_engine',
    json_struct = {
        'product_name': 'name',
        'product_category': 'category',
        'product_price': 'price'
    },
    input_text = 'description';
```

```sql
SELECT json
FROM product_extract_json
WHERE description = "
What is Rabbit R1?
The Rabbit R1 is a pocket-sized AI device that promises a simpler and more intuitive way to interact with technology. Instead of being app-driven, the device relies on an AI model called LAMB (large action model) to understand your instructions and complete tasks autonomously.
The device has a bright orange body, and is small and lightweight with a touchscreen, scroll wheel, and a talk button. There is also a rotating camera that functions as eyes of the device. IT provides all of this just for 300 dollars.

The Rabbit R1 runs on its own operating system, called the Rabbit OS, that eliminates the need for app stores and downloads, requiring only natural language voice input to navigate. The initial version supports integration with the likes of Uber, Spotify, and Amazon, with the AI able to train and learn using other apps in the future.
";
```

The output is:


| json |
| ---- |
| {"name": "Rabbit R1", "category": "AI device", "price": "300 dollars"} |