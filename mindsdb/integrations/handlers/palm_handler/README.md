# PaLM API Handler

This is the integration of PaLM2 API for MindsDB.

## PaLM API

[PaLM API](https://developers.generativeai.google/products/palm) is used to build generative AI applications for use cases like content generation, dialog agents, summarization, classification, and more


## Implementation

This handler uses `google-generativeai` library to connect to the PaLM API.

The required arguments to establish a connection are:

* `api_key`: the api key for authenticate with the PaLM API
either
* `question_column`: the name of the column in the dataset to be used as input
or
* `prompt_template`: the prompt template to be used as placeholder


## Usage

In order to make use of this handler and query the PaLM API, the following syntax can be used:

### Question Answering prompt

You can  create a model by providing a name for table of the question and answers to query later.

```sql
-- you might already have a database
CREATE DATABASE palm_dev;

CREATE MODEL palm_dev.model_name
PREDICT answer
USING
    engine="palm",
    mode = "default",
    question_column = "question",
    api_key = "YOUR_API_KEY";
```

Now, this model could be used to query as a prompt to the PaLM as follows:

```sql
SELECT
    question,
    answer
FROM
    palm_dev.model_name
WHERE
    question = "What is mindsdb?";
```

### Placeholder/Template prompt

You can create a model with a placeholder/template prompt to query later with the given values for the placeholder keys.

```sql
CREATE MODEL palm_dev.model_name
PREDICT answer
USING
    engine="palm",
    mode = "default",
    prompt_template = "list some facts about {{ thing }}",
    api_key = "YOUR_API_KEY";
```

Now, this model can be used by providing the values of the keys in the `prompt_template` as follows:

```sql
SELECT
    *
FROM
    palm_dev.model_name
WHERE
    thing = "mindsdb";
```

### Embeddings

You can create a model that generates the emebeddings of the given text.

```sql
CREATE MODEL palm_dev.model_name
PREDICT answer
USING
    engine="palm",
    mode = "embedding",
    question_column = "question",
    api_key = "YOUR_API_KEY";
```

Then, this model can be queried to get the embeddings for the given text as follows:

```sql
SELECT
    *
FROM
    palm_dev.model_name
WHERE
    question = "What is mindsdb?";
```

### User Input and Prompt

You can create a model for a specific prompt and specifically ask the user for input with a given context.

```sql
CREATE MODEL palm_dev.model_name
PREDICT answer
USING
    engine = 'palm',
    prompt = 'tell some joke about programming',
    user_column = 'user_input';
    api_key = 'YOUR_API_KEY';
```

Then, this model can be queried to get the answer as follows:

```sql
SELECT
    *
FROM
    palm_dev.model_name
WHERE
    user_input = 'python';
```

## Features Implemented

- [x] Question Answering
- [x] Placeholder/Template
- [x] Embeddings
- [x] User Input Prompts

## TODOs

- [ ] Conversational chat prompts
- [ ] Moderation of Prompts
