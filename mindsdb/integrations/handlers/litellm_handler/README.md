## LiteLLM Handler

This documentation describes the integration of MindsDB with [LiteLLM](https://docs.litellm.ai/docs/), that allows you to call all LLM APIs using the OpenAI format.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use LiteLLM within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).


## Setup

Create an AI engine from the [LiteLLM handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/litellm_handler).

```sql
CREATE ML_ENGINE litellm
FROM litellm;
```


## Usage

Here is how to create a model that answers questions:

```sql
CREATE MODEL chat_model
PREDICT text
USING
    engine="litellm",
    model="gpt-4",
    api_key="openai-api-key";
```

Query the model to get answer:

```sql
SELECT *
FROM mindsdb.chat_model
WHERE question = "How far is the moon?"
```

Here is the output:

```sql
+---------------------------+-------------------------------+
|question                   |answer                         |
+---------------------------+-------------------------------+
|How far is the moon?   |    384,400 km                     |
+---------------------------+-------------------------------+

```

<Tip>

**Next Steps**
Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>

