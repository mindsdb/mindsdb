---
title: Portkey
sidebarTitle: Portkey
---

This documentation describes the integration of MindsDB with [Portkey](https://www.portkey.com/), an AI Gateway that allows developers to connect to All the AI models in the world with a single API.
Portkey also brings in observability, caching, and other features that are useful for building production-grade AI applications.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Portkey within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Portkey API key required to deploy and use Portkey within MindsDB. Follow the [instructions for obtaining the API key](https://docs.portkey.ai/docs/api-reference/introduction).

## Setup

Create an AI engine from the [Portkey handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/portkey_handler).

<Info>
You can pass all the parameters that are supported by Portkey inside the `USING` clause.
</Info>


```sql
CREATE ML_ENGINE portkey_engine
FROM portkey
USING
    portkey_api_key = '{PORTKEY_API_KEY}',
    config = '{PORTKEY_CONFIG_ID}';
```

Create a model using `portkey_engine` as an engine.
<Info>
You can pass all the parameters supported by Portkey Chat completions here inside the `USING` clause.
</Info>

```sql
CREATE MODEL portkey_model
PREDICT answer
USING
      engine = 'portkey_engine',
      temperature = 0.2;
</Info>``

<Info>

The integrations between Portkey and MindsDB was implemented using [Portkey Python SDK](https://docs.portkey.ai/docs/api-reference/portkey-sdk-client).
</Info>

Query the model to get predictions.

```sql
SELECT question, answer
FROM portkey_model
WHERE question = 'Where is Stockholm located?';
```

Here is the output:

```sql
+-----------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
| question                    | answer                                                                                                                                             |
+-----------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
| Where is Stockholm located? |  Stockholm is the capital and largest city of Sweden. It is located on Sweden's south-central east coast, where Lake Mälaren meets the Baltic Sea. |
+-----------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>
