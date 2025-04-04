---
title: Azure OpenAI
sidebarTitle: Azure OpenAI
---

This documentation describes the integration of MindsDB with [Azure OpenAI](https://learn.microsoft.com/en-us/azure/cognitive-services/openai/overview), a service from Microsoft that offers access to OpenAI models like GPT-4, GPT-3.5, and Embeddings through Azure infrastructure.

The integration enables deploying Azure OpenAI models within MindsDB, allowing them to interact with data from various sources via SQL.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Azure OpenAI within MindsDB, install the required dependencies by following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. You must have:
   - An Azure subscription.
   - A deployed model in Azure OpenAI (e.g., `text-embedding-3-large`, `gpt-4`).
   - The **API key**, **endpoint**, and optionally **API version** from your Azure OpenAI resource.

## Setup

Create an AI engine using the `azure_openai` handler:

```sql
CREATE ML_ENGINE azure_openai_engine
FROM azure_openai
USING
    azure_openai_api_key = 'your-api-key';
```

Then, create a model using this engine:
```sql
CREATE MODEL azure_openai_model
PREDICT result
USING
    engine = 'azure_openai_engine',
    api_base = 'https://<your-endpoint>.openai.azure.com/',
    model_name = 'your-deployment-name',
    mode = 'embedding',
    question_column = 'text';
```

## Usage

Embedding Example

```sql
CREATE MODEL azure_embeddings
PREDICT embedding
USING
    engine = 'azure_openai_engine',
    mode = 'embedding',
    model_name = 'text-embedding-3-large',
    question_column = 'text';
```

Query the model:

```sql
SELECT text, embedding
FROM azure_embeddings
WHERE text = 'The sun is a star.';
```

Expected output:
```sql
+------------------------+------------------------------+
|text                    |embedding                     |
+------------------------+------------------------------+
|The sun is a star.      |[0.124, 0.532, ..., 0.091]    |
+------------------------+------------------------------+
```
