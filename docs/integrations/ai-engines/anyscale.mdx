---
title: Anyscale Endpoints
sidebarTitle: Anyscale Endpoints
---

This documentation describes the integration of MindsDB with [Anyscale Endpoints](https://www.anyscale.com/endpoints), a fast and scalable API to integrate OSS LLMs into apps.
The integration allows for the deployment of Anyscale Endpoints models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Anyscale Endpoints within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the Anyscale Endpoints API key required to deploy and use Anyscale Endpoints models within MindsDB. Follow the [instructions for obtaining the API key](https://docs.endpoints.anyscale.com/guides/authenticate/).

## Setup

Create an AI engine from the [Anyscale Endpoints handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/anyscale_endpoints_handler).

```sql
CREATE ML_ENGINE anyscale_endpoints_engine
FROM anyscale_endpoints
USING
      anyscale_endpoints_api_key = 'api-key-value';
```

Create a model using `anyscale_endpoints_engine` as an engine.

```sql
CREATE MODEL anyscale_endpoints_model
[FROM integration
         (SELECT * FROM table)]
PREDICT target_column
USING
      engine = 'anyscale_endpoints_engine',   -- engine name as created via CREATE ML_ENGINE
      model_name = 'model-name',              -- choose one of available models
      prompt_teplate = 'prompt-to-the-model'; -- prompt message to be completed by the model
```

<Note>

The implementation is based on the engine for the OpenAI API, as Anyscale conforms to it. There are a few notable differences, though:

1. All models supported by Anyscale Endpoints are open source. A full list can be found [here for inference-only under section *Supported models*](https://app.endpoints.anyscale.com/docs).
2. Not every model is supported for fine-tuning. You can find a list [here under section *Fine Tuning - Supported models*](https://app.endpoints.anyscale.com/docs).

*Please check both lists regularly, as they are subject to change. If you try to fine-tune a model that is not supported, you will get a warning and subsequently an error from the Anyscale endpoint.*

3. This integration only offers chat-based text completion models, either for *normal* text or specialized for code.
4. When providing a description, this integration returns the respective HuggingFace model card.
5. Fine-tuning requires that your dataset complies with the chat format. That is, each row should contain a context and a role. The context is the text that is the message in the chat, and the role is who authored it (system, user, or assistant, where the last one is the model). For more information, please check the [fine tuning guide in the Anyscale Endpoints docs](https://app.endpoints.anyscale.com/docs).
</Note>

<Info>

The base URL for this API is `https://api.endpoints.anyscale.com/v1`.
</Info>

## Usage

The following usage examples utilize `anyscale_endpoints_engine` to create a model with the `CREATE MODEL` statement.

Classify text sentiment using the Mistral 7B model.

```sql
CREATE MODEL anyscale_endpoints_model
PREDICT sentiment
USING
   engine = 'anyscale_endpoints_engine',
   model_name = 'mistralai/Mistral-7B-Instruct-v0.1',
   prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
```

Query the model to get predictions.

```sql
SELECT text, sentiment
FROM anyscale_endpoints_model
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

<Tip>

**Next Steps**

Follow [this tutorial](https://docs.mindsdb.com/finetune/anyscale) to see more use case examples.
</Tip>
