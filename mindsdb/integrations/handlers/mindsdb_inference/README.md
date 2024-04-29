---
title: MindsDB Inference Endpoints
sidebarTitle: MindsDB Inference Endpoints
---

This documentation describes the integration of MindsDB with [MindsDB Inference Endpoints](https://mindsdb-docs.hashnode.space/), a cloud service that simplifies the way developers interact with cutting-edge LLMs through a universal API.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use MindsDB Inference Endpoints within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the MindsDB Inference API key required to deploy and use MindsDB Inference Endpoints models within MindsDB. Follow the [instructions for obtaining the API key](https://mindsdb-docs.hashnode.space/docs/authentication).

## Setup

Create an AI engine from the [MindsDB Inference Endpoints handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mindsdb_inference_handler).

```sql
CREATE ML_ENGINE mindsdb_serve
FROM mindsdb_inference
USING
      mindsdb_inference_api_key = 'api-key-value'
```

Create a model using `mindsdb_serve` as an engine.

```sql
CREATE MODEL mindsdb_inference_model
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
CREATE MODEL mindsdb_inference_model
PREDICT sentiment
USING
   engine = 'mindsdb_serve',
   model_name = 'mistral-7b',
   prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
```

Query the model to get predictions.

```sql
SELECT text, sentiment
FROM mindsdb_inference_model
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
