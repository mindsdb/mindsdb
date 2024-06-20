---
title: LangChain
sidebarTitle: LangChain
---

This documentation describes the integration of MindsDB with [LangChain](https://www.langchain.com/), a framework for developing applications powered by language models.
The integration allows for the deployment of LangChain models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use LangChain within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the API key for a selected model (provider) that you want to use through LangChain.

<Info>

Available models include the following:

- Anthropic ([how to get the API key](https://docs.anthropic.com/claude/docs/getting-access-to-claude))
- OpenAI ([how to get the API key](https://help.openai.com/en/articles/4936850-where-do-i-find-my-openai-api-key))
- Anyscale ([how to get the API key](https://docs.endpoints.anyscale.com/guides/authenticate/))
- Ollama ([how to download Ollama](https://ollama.com/download))

The LiteLLM model provider is available in MindsDB Cloud only. Use the MindsDB API key, which can be generated in the MindsDB Cloud editor at `cloud.mindsdb.com/account`.
</Info>

## Setup

Create an AI engine from the [LangChain handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/langchain_handler).

```sql
CREATE ML_ENGINE langchain_engine
FROM langchain
[USING
      serper_api_key = 'your-serper-api-key', -- it is an optional parameter (if provided, the model will use serper.dev search to enhance the output)

      -- provide one of the below parameters
      anthropic_api_key = 'api-key-value',
      anyscale_api_key = 'api-key-value',
      litellm_api_key = 'api-key-value',
      openai_api_key = 'api-key-value'];
```

Create a model using `langchain_engine` as an engine and one of OpenAI/Anthropic/Anyscale/LiteLLM as a model provider.

```sql
CREATE MODEL langchain_model
PREDICT target_column
USING
      engine = 'langchain_engine',           -- engine name as created via CREATE ML_ENGINE
      <provider>_api_key = 'api-key-value',  -- if not provided in CREATE ML_ENGINE (replace <provider> with one of the available values)
      model_name = 'model-name',             -- optional, model to be used (for example, 'gpt-4' if 'openai_api_key' provided)
      prompt_template = 'message to the model that may include some {{input}} columns as variables';
```

<Info>

This handler supports [tracing features for LangChain via LangFuse](https://langfuse.com/docs/integrations/langchain/tracing). To use it, provide the following parameters in the `USING` clause:

- `langfuse_host`,
- `langfuse_public_key`,
- `langfuse_secret_key`.
</Info>

<Tip>

`Agents` and `Tools` are some of the main abstractions that LangChain offers. You can read more about them in the [LangChain documentation](https://python.langchain.com/docs/modules/agents/).
</Tip>

<Info>

There are three different tools utilized by this agent:

* **MindsDB** is the internal MindsDB executor.
* **Metadata** fetches the metadata information for the available tables.
* **Write** is able to write agent responses into a MindsDB data source.

Each tool exposes the internal MindsDB executor in a different way to perform its tasks, effectively enabling the agent model to read from (and potentially write to) data sources or models available in the active MindsDB project.
</Info>

Create a conversational model using `langchain_engine` as an engine and one of OpenAI/Anthropic/Anyscale/LiteLLM as a model provider.

<AccordionGroup>

<Accordion title="OpenAI">
```sql
CREATE MODEL langchain_openai_model
PREDICT answer
USING
     engine = 'langchain_engine',       -- engine name as created via CREATE ML_ENGINE
     provider = 'openai',               -- one of the available providers
     openai_api_key = 'api-key-value',  -- if not provided in CREATE ML_ENGINE
     model_name = 'gpt-3.5-turbo',      -- choose one of the available OpenAI models
     mode = 'conversational',           -- conversational mode
     user_column = 'question',          -- column name that stores input from the user
     assistant_column = 'answer',       -- column name that stores output of the model (see PREDICT column)
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way';
```
</Accordion>

<Accordion title="Anthropic">
```sql
CREATE MODEL langchain_openai_model
PREDICT answer
USING
     engine = 'langchain_engine',          -- engine name as created via CREATE ML_ENGINE
     provider = 'anthropic',               -- one of the available providers
     anthropic_api_key = 'api-key-value',  -- if not provided in CREATE ML_ENGINE
     model_name = 'claude-2.1',            -- choose one of the available OpenAI models
     mode = 'conversational',              -- conversational mode
     user_column = 'question',             -- column name that stores input from the user
     assistant_column = 'answer',          -- column name that stores output of the model (see PREDICT column)
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way';
```
</Accordion>

<Accordion title="Anyscale">
```sql
CREATE MODEL langchain_anyscale_model
PREDICT answer 
USING
     engine = 'langchain_engine',                        -- engine name as created via CREATE ML_ENGINE
     provider = 'anyscale',                              -- one of the available providers
     anyscale_api_key = 'api-key-value',                 -- if not provided in CREATE ML_ENGINE
     model_name = 'mistralai/Mistral-7B-Instruct-v0.1',  -- choose one of the models available from Anyscale
     mode = 'conversational',           -- conversational mode
     user_column = 'question',          -- column name that stores input from the user
     assistant_column = 'answer',       -- column name that stores output of the model (see PREDICT column) 
     base_url = 'https://api.endpoints.anyscale.com/v1',
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way';
```
</Accordion>

<Accordion title="LiteLLM">
```sql
CREATE MODEL langchain_litellm_model
PREDICT answer 
USING
     engine = 'langchain_engine',            -- engine name as created via CREATE ML_ENGINE
     provider = 'litellm',                   -- one of the available providers
     litellm_api_key = 'api-key-value',      -- if not provided in CREATE ML_ENGINE
     model_name = 'assistant',               -- model created in MindsDB
     mode = 'conversational',                -- conversational mode
     user_column = 'question',               -- column name that stores input from the user
     assistant_column = 'answer',            -- column name that stores output of the model (see PREDICT column)
     base_url = 'https://ai.dev.mindsdb.com',
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way';
```
</Accordion>

<Accordion title="Mindsdb">

Using mindsdb model in langchain:
```sql
CREATE MODEL langchain_mindsdb_model
PREDICT answer
USING
     engine = 'langchain_engine',       -- engine name as created via CREATE ML_ENGINE
     provider = 'mindsdb',              -- one of the available providers
     model_name = 'gpt_model',          -- mindsdb model
     mode = 'conversational',           -- conversational mode
     user_column = 'question',          -- column name that stores input from the user
     assistant_column = 'answer',       -- column name that stores output of the model (see PREDICT column)
     verbose = True,
     prompt_template = 'Answer the user input in a helpful way';
```
</Accordion>

</AccordionGroup>

## Usage

The following usage examples utilize `langchain_engine` to create a model with the `CREATE MODEL` statement.

Create a model that will be used to describe, analyze, and retrieve.

```sql
CREATE MODEL tool_based_agent
PREDICT completion
USING
    engine = 'langchain_engine',
    prompt_template = 'Answer the users input in a helpful way: {{input}}';
```

Here, we create the `tool_based_agent` model using the LangChain engine, as defined in the `engine` parameter. This model answers users' questions in a helpful way, as defined in the `prompt_template` parameter, which specifies `input` as the input column when calling the model.

### Describe data

Query the model to describe data.

```sql
SELECT input, completion
FROM tool_based_agent
WHERE input = 'Could you describe the `mysql_demo_db.house_sales` table please?'
USING
    verbose = True,
    tools = [],
    max_iterations = 10;
```

Here is the output:

```sql
The `mysql_demo_db.house_sales` table is a base table that contains information related to house sales. It has the following columns:
- `saledate`: of type text, which likely contains the date when the sale was made.
- `house_price_moving_average`: of type int, which might represent a moving average of house prices, possibly to track price trends over time.
- `type`: of type text, which could describe the type of house sold.
- `bedrooms`: of type int, indicating the number of bedrooms in the sold house.
```

To get information about the `mysql_demo_db.house_sales` table, the agent uses the Metadata tool. Then the agent prepares the response.

### Analyze data

Query the model to analyze data.

```sql
SELECT input, completion
FROM tool_based_agent
WHERE input = 'I want to know the average number of rooms in the downtown neighborhood as per the `mysql_demo_db.home_rentals` table'
USING
    verbose = True,
    tools = [],
    max_iterations = 10;
```

Here is the output:

```sql
The average number of rooms in the downtown neighborhood, as per the `mysql_demo_db.home_rentals` table, is 1.6 rooms.
```

Here, the model uses the Metadata tool again to fetch the column information. As there is no `beds` column in the `mysql_demo_db.home_rentals` table, it uses the `number_of_rooms` column and writes the following query:

```sql
SELECT AVG(number_of_rooms)
FROM mysql_demo_db.home_rentals
WHERE neighborhood = 'downtown';
```

This query returns the value of 1.6, which is then used to write an answer.

### Retrieve data

Query the model to retrieve data.

```sql
SELECT input, completion
FROM tool_based_agent
WHERE input = 'There is a property in the south_side neighborhood with an initial price of 2543 the `mysql_demo_db.home_rentals` table. What are some other details of this listing?'
USING
    verbose = True,
    tools = [],
    max_iterations = 10;
```

Here is the output:

```sql
The property in the `south_side` neighborhood with an initial price of 2543 has the following details:
- Number of rooms: 1
- Number of bathrooms: 1
- Square footage (sqft): 630
- Location: great
- Days on market: 11
- Initial price: 2543
- Neighborhood: south_side
- Rental price: 2543.0
```

Here, the model uses the Metadata tool again to fetch information about the table. Then, it creates and executes the following query:

```sql
SELECT *
FROM mysql_demo_db.home_rentals
WHERE neighborhood = 'south_side'
AND initial_price = 2543;
```

On execution, the model gets this output:

```sql
+---------------+-------------------+----+--------+--------------+-------------+------------+------------+
|number_of_rooms|number_of_bathrooms|sqft|location|days_on_market|initial_price|neighborhood|rental_price|
+---------------+-------------------+----+--------+--------------+-------------+------------+------------+
|1              |1                  |630 |great   |11            |2543         |south_side  |2543        |
+---------------+-------------------+----+--------+--------------+-------------+------------+------------+
```

Consequently, it takes the query output and writes an answer.

<Tip>

**Next Steps**

Go to the [Use Cases](https://docs.mindsdb.com/use-cases/overview) section to see more examples.
</Tip>
