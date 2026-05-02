---
title: MiniMax
sidebarTitle: MiniMax
---

This documentation describes the integration of MindsDB with [MiniMax](https://www.minimax.io/), a leading AI company providing state-of-the-art language models through its API.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use MiniMax within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the MiniMax API key required to deploy and use MiniMax models within MindsDB. Get the API key from [MiniMax platform](https://platform.minimax.io/).

## Setup

Create an AI engine from the [MiniMax handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/minimax_handler).

```sql
CREATE ML_ENGINE minimax_engine
FROM minimax
USING
      minimax_api_key = 'api-key-value';
```

Create a model using `minimax_engine` as an engine.

```sql
CREATE MODEL minimax_model
PREDICT answer
USING
      engine = 'minimax_engine',          -- engine name as created via CREATE ML_ENGINE
      model_name = 'MiniMax-M2.7',        -- choose one of available models
      prompt_template = 'prompt-to-the-model'; -- prompt message to be completed by the model
```

## Usage

The following usage examples utilize `minimax_engine` to create a model with the `CREATE MODEL` statement.

Classify text sentiment using the MiniMax-M2.7 model.

```sql
CREATE MODEL minimax_model
PREDICT sentiment
USING
   engine = 'minimax_engine',
   model_name = 'MiniMax-M2.7',
   prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}. Give sentiment as result only.';
```

Query the model to get predictions.

```sql
SELECT text, sentiment
FROM minimax_model
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

| Model | Description |
|-------|-------------|
| `MiniMax-M2.7` | Peak Performance. Ultimate Value. Master the Complex. Default model. |
| `MiniMax-M2.7-highspeed` | Same performance, faster and more agile. |

For the latest model information, visit the [MiniMax API documentation](https://platform.minimax.io/docs/api-reference/text-openai-api).

## Troubleshooting Guide

<Warning>
`SQL statement cannot be parsed by mindsdb_sql`

* **Symptoms**: SQL queries failing or not recognizing table names containing spaces or special characters.
* **Checklist**:
    1. Ensure table names with spaces or special characters are enclosed in backticks.
    Examples:
        * Correct:

            ```sql
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN minimax_model AS output
            ```

</Warning>
