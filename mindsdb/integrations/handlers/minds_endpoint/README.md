---
title: Minds Endpoint
sidebarTitle: Minds Endpoint
---

This documentation describes the integration of MindsDB with [Minds Endpoint](https://mindsdb-docs.hashnode.space/), a cloud service that simplifies the way developers interact with cutting-edge LLMs through a universal API.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Minds Endpoint within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Minds Endpoint API key required to deploy and use Minds Endpoint models within MindsDB. Follow the [instructions for obtaining the API key](https://mindsdb-docs.hashnode.space/docs/authentication).

## Setup

Create an AI engine from the [Minds Endpoint handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/minds_endpoint_handler).

```sql
CREATE ML_ENGINE mindsdb_serve
FROM minds_endpoint
USING
      minds_endpoint_api_key = 'api-key-value'
```

Create a model using `mindsdb_serve` as an engine.

```sql
CREATE MODEL minds_endpoint_model
PREDICT answer
USING
      engine = 'mindsdb_serve',   -- engine name as created via CREATE ML_ENGINE
      model_name = 'model-name',              -- choose one of available models
      prompt_template = 'prompt-to-the-model' -- prompt message to be completed by the model
```

## Usage

The following usage examples utilize `mindsdb_serve` to create a model with the `CREATE MODEL` statement.

Classify text sentiment using the Mistral 7B model.

```sql
CREATE MODEL minds_endpoint_model
PREDICT sentiment
USING
   engine = 'mindsdb_serve',
   model_name = 'mistral-7b',
   prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
```

Query the model to get predictions.

```sql
SELECT text, sentiment
FROM minds_endpoint_model
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

For an overview of the models supported, visit the [following docs](https://docs.mdb.ai/). This list will help you quickly identify the right models for your needs.

## Troubleshooting Guide

<Warning>
`Authentication Error`

* **Symptoms**: Failure to authenticate to Minds Endpoint.
* **Checklist**:
    1. Make sure that your Minds Endpoint account is active.
    2. Confirm that your API key is correct.
    3. Ensure that your API key has not been revoked.
</Warning>

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
            JOIN minds_endpoint_model AS output
            ```
        * Incorrect: 
            ```sql
            SELECT input.text, output.sentiment
            FROM integration.'travel data' AS input
            JOIN minds_endpoint_model AS output
            ```
        * Correct:  
            ```sql 
            SELECT input.text, output.sentiment
            FROM integration.`travel data` AS input
            JOIN minds_endpoint_model AS output
            ```
</Warning>