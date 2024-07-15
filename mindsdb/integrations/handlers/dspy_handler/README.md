---
title: DSPy
sidebarTitle: DSPy
---

This documentation describes the integration of MindsDB with [DSPy](https://dspy-docs.vercel.app/docs/intro), a framework for developing applications powered by language models. The integration allows for the deployment of DSPy models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use DSPy within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the API key for a selected model (provider) that you want to use through DSPy.

<Info>

Available models include the following:

- OpenAI ([how to get the API key](https://help.openai.com/en/articles/4936850-where-do-i-find-my-openai-api-key))

</Info>

## Setup

Create an AI engine from the DSPy Handler

```sql
CREATE ML_ENGINE dspy_engine
FROM dspy
[USING
      openai_api_key = 'api-key-value'];
```

<Tip>

DSPy was created to optimize prompts to improve the performance of LLMs. You can read more about this in the [DSPy documentation](https://dspy-docs.vercel.app/docs/deep-dive/language_model_clients/remote_models/OpenAI).
</Tip>


Create a conversational model using `dspy_engine` as an engine and OpenAI as a model provider.  To get started, connect to this database to get the initial dataset:

```sql
CREATE DATABASE dspy_data
WITH ENGINE = "postgres",
PARAMETERS = {
"user": "demo_user",
"password": "demo_password",
"host": "samples.mindsdb.com",
"port": "5432",
"database": "postgres",
"schema": "dspy_data"
};
```


This dataset contains a prompt template filled with some examples from the Spyder dataset.  The prompt template is used to perform a text to SQL question and answer format query.  This database containts the dataset: `dspy_train_dataset`.  This will serve as a warm start dataset for DSPy to improve on.


<Tip>
OpenAI:

```sql
CREATE MODEL dspy_openai_model
FROM dspy_data (SELECT input as question, output as answer FROM dspy_train_dataset)
PREDICT answer
USING
     engine = 'dspy_engine',       -- engine name as created via CREATE ML_ENGINE
     provider = 'openai',               -- one of the available providers
     openai_api_key = 'api-key-value',  -- if not provided in CREATE ML_ENGINE
     model_name = 'gpt-3.5-turbo',      -- choose one of the available OpenAI models
     mode = 'conversational',           -- conversational mode
     user_column = 'question',          -- column name that stores input from the user
     assistant_column = 'answer',       -- column name that stores output of the model (see PREDICT column)
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way: {{question}}';
```
<Tip>

## Usage

The following usage example utilizes `dspy_engine` to create a model with the `CREATE MODEL` statement.

```sql
SELECT question, answer
FROM dspy_openai_model
WHERE question = 'How many departments are led by heads who are not mentioned?';
```

Here, we create the `dspy_openai_model` model using the DSPy engine, as defined in the `engine` parameter. This model answers users' questions in a helpful way, as defined in the `prompt_template` parameter, which specifies `input` as the input question when calling the model.  In this case, the question is asking about data from the cold start dataframe, but any question can be asked and will be stored and utilized by DSPy to answer future queries.
