---
title: Ollama
sidebarTitle: Ollama
---

This documentation describes the integration of MindsDB with [Ollama](https://ollama.com/), a tool that enables local deployment of large language models.
The integration allows for the deployment of Ollama models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Ollama within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Follow [this instruction](https://github.com/ollama/ollama?tab=readme-ov-file#ollama) to download Ollama and run models locally.

<Info>
Here are the recommended system specifications:

- A working Ollama installation, as in point 3.
- For 7B models, at least 8GB RAM is recommended.
- For 13B models, at least 16GB RAM is recommended.
- For 70B models, at least 64GB RAM is recommended.
  </Info>

## Setup

Create an AI engine from the [Ollama handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/ollama_handler).

```sql
CREATE ML_ENGINE ollama_engine
FROM ollama;
```

Create a model using `ollama_engine` as an engine.

```sql
CREATE MODEL ollama_model
PREDICT completion
USING
   engine = 'ollama_engine',   -- engine name as created via CREATE ML_ENGINE
   model_name = 'model-name',  -- model run with 'ollama run model-name'
   ollama_serve_url = 'http://localhost:11434';
```

<Tip>
If you run Ollama and MindsDB in separate Docker containers, use the `localhost` value of the container. For example, `ollama_serve_url = 'http://host.docker.internal:11434'`.
</Tip>

You can find [available models here](https://github.com/ollama/ollama?tab=readme-ov-file#model-library).

## Usage

The following usage examples utilize `ollama_engine` to create a model with the `CREATE MODEL` statement.

Deploy and use the `llama2` model.

First, [download Ollama](https://github.com/ollama/ollama?tab=readme-ov-file#ollama) and run the model locally by executing `ollama run llama2`.

Now deploy this model within MindsDB.

```sql
CREATE MODEL llama2_model
PREDICT completion
USING
   engine = 'ollama_engine',
   model_name = 'llama2';
```

<Tip>
Models can be run in either the 'generate' or 'embedding' modes. The 'generate' mode is used for text generation, while the 'embedding' mode is used to generate embeddings for text.

However, these modes can only be used with models that support them. For example, the `moondream` model supports both modes.

By default, if the mode is not specified, the model will run in 'generate' mode if multiple modes are supported. If only one mode is supported, the model will run in that mode.

To specify the mode, use the `mode` parameter in the `CREATE MODEL` statement. For example, `mode = 'embedding'`.
</Tip>

Query the model to get predictions.

```sql
SELECT text, completion
FROM llama2_model
WHERE text = 'Hello';
```

Here is the output:

```sql
+-------+------------+
| text  | completion |
+-------+------------+
| Hello | Hello!     |
+-------+------------+
```

You can override the prompt message as below:

```sql
SELECT text, completion
FROM llama2_model
WHERE text = 'Hello'
USING
   prompt_template = 'Answer using exactly five words: {{text}}:';
```

Here is the output:

```sql
+-------+------------------------------------+
| text  | completion                         |
+-------+------------------------------------+
| Hello | Hello! *smiles* How are you today? |
+-------+------------------------------------+
```

<Tip>
**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>

### Embeddings

If you want to use an embedding model (instead of text generation), then you will want to activate embedding mode at model creation:

```sql
CREATE MODEL nomic_embed_model
PREDICT embeddings
USING
   engine = 'ollama_engine',
   model_name = 'nomic-embed-text',
   mode = 'embedding',
   prompt_template = '{{column}}, {{another_column}}';
```

The output will contain embeddings for each input row (length is model-dependent):

```sql
+-------+---------------------------------------------------------------------------------+
| column  | another_column  | embeddings                                                  |
+-------+---------------------------------------------------------------------------------+
| Hello   | Matt!           | [0.7849581241607666,1.263154149055481,-4.024246692657471... |
+-------+---------------------------------------------------------------------------------+
```
