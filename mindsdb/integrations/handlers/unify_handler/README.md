---
title: Unify
sidebarTitle: Unify
---

This documentation describes the integration of MindsDB with [Unify](https://unify.ai/), a technology company focused on unifying artificial intelligence.
The integration allows for using Unify LLM endpoints within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Unify within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Unify API key required to use Unify endpoints within MindsDB. Sign up for a Unify account and request an API key from the Unify dashboard.

## Setup

Create an AI engine from the [Unify handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/unify_handler).

```sql
CREATE ML_ENGINE unify_engine
FROM unify
USING
      unify_api_key = 'your-unify-api-key';
```

Create a model using `unify_engine` as an engine.

```sql
CREATE MODEL unify_model
PREDICT target_column
USING
      engine = 'unify_engine',      -- engine name as created via CREATE ML_ENGINE
      model = 'model_name',         -- LLM name
      provider = 'provider_name',   -- LLM provider name
      question_column = 'column_name';       -- column that stores input/question to the model
```

## Usage

The following usage examples utilize `unify_engine` to create a model with the `CREATE MODEL` statement.

Create a model to answer to a given instruction.

```sql
CREATE MODEL unify_model
PREDICT output
USING
      engine = 'unify_engine',
      model = 'llama-3-8b-chat',
      provider = 'together-ai',
      column = 'text';
```

Where:

| Name       | Description                                           |
| ---------- | ----------------------------------------------------- |
| `model`    | It defines the model (LLM) name to use.               |
| `provider` | It defines the model (LLM) provider.                  |
| `column`   | It defines the column with the text to be acted upon. |

Query the model to get predictions.

```sql
SELECT text, output
FROM unify_model
WHERE text = 'Hello';
```

Here is the output:

```
+-------+---------------------------------------------------------------------------------------------------+
| text  | language                                                                                          |
+-------+---------------------------------------------------------------------------------------------------+
| Hello | Hello! It's nice to meet you. Is there something I can help you with, or would you like to chat?  |
+-------+---------------------------------------------------------------------------------------------------+
```

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>