---
title: Hugging Face
sidebarTitle: Hugging Face
---

This documentation describes the integration of MindsDB with [Hugging Face](https://huggingface.co/), a company that develops computer tools for building applications using machine learning.
The integration allows for the deployment of Hugging Face models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Hugging Face within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Setup

Create an AI engine from the [Hugging Face handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/huggingface_handler).

```sql
CREATE ML_ENGINE huggingface_engine
FROM huggingface
USING huggingface_api_api_key = 'hf_xxx';
```

Create a model using `huggingface_engine` as an engine.

```sql
CREATE MODEL huggingface_model
PREDICT target_column
USING
      engine = 'huggingface_engine',     -- engine name as created via CREATE ML_ENGINE
      model_name = 'hf_hub_model_name',  -- choose one of PyTorch models from the Hugging Face Hub
      task = 'task_name',                -- choose one of 'text-classification', 'text-generation', 'zero-shot-classification', 'translation', 'summarization', 'text2text-generation', 'fill-mask'
      input_column = 'column_name',      -- column that stores input/question to the model
      labels = ['label 1', 'label 2'];   -- labels used to classify data (used for classification tasks)
```

## Usage

The following usage examples utilize `huggingface_engine` to create a model with the `CREATE MODEL` statement.

Create a model to classify input text as spam or ham.

```sql
CREATE MODEL spam_classifier
PREDICT spam_or_ham
USING
      engine = 'huggingface_engine',
      model_name = 'mrm8488/bert-tiny-finetuned-sms-spam-detection',
      task = 'text-classification',
      input_column = 'text',
      labels = ['ham', 'spam'];
```

Query the model to get predictions.

```sql
SELECT text, spam_or_ham
FROM spam_classifier
WHERE text = 'Subscribe to this channel asap';
```

Here is the output:

```sql
+--------------------------------+-------------+
| text                           | spam_or_ham |
+--------------------------------+-------------+
| Subscribe to this channel asap | spam        |
+--------------------------------+-------------+
```

<Tip>

**Next Steps**

Follow [this link](https://docs.mindsdb.com/sql/tutorials/hugging-face-examples) to see more use case examples.
</Tip>
