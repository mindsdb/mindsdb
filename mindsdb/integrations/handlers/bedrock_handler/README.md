---
title: Amazon Bedrock
sidebarTitle: Amazon Bedrock
---

This documentation describes the integration of MindsDB with [Amazon Bedrock](https://aws.amazon.com/bedrock/), a fully managed service that offers a choice of high-performing foundation models (FMs) from leading AI companies.
The integration allows for the deployment of models offered by Amazon Bedrock within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Amaon Bedrock within MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Obtain the AWS credentials for a user with access to the Amazon Bedrock service.

## Setup

Create an AI engine from the [Amazon Bedrock handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/bedrock_handler).

```sql
CREATE ML_ENGINE bedrock_engine
FROM bedrock
USING
    aws_access_key_id = 'AQAXEQK89OX07YS34OP',
    aws_secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    aws_session_token = 'FwoGZXIvYXdzEJr...',
    region_name = 'us-east-1';
```

Required parameters for creating an engine include the following:

- `aws_access_key_id`: The AWS access key ID for the user.
- `aws_secret_access_key`: The AWS secret access key for the user.
- `region_name`: The AWS region to use.

Optional parameters include the following:

- `aws_session_token`: The AWS session token for the user. This is required when using temporary security credentials.

Create a model using `bedrock_engine` as an engine.

```sql
CREATE MODEL bedrock_model
PREDICT answer
USING
    engine = 'bedrock_engine',
    question_column = 'question',
    max_tokens = 100,
    temperature = 0.3;
```

Required parameters for creating a model include the following:

- `engine`: The name of the engine created via `CREATE ML_ENGINE`.

Optional parameters include the following:

- `mode`: The mode to run inference in. The default mode is `default` and the other supported mode is `conversational`.
- `model_id`: The model ID to use for inference. The default model ID is `amazon.titan-text-premier-v1:0` and a list of other supported models can be found https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html.
- `question_column`: The column that stores the user input.
- `context_column`: The column that stores context to the user input.
- `prompt_template`: A template for the prompt with placeholders to be replaced by the user input.
- `max_tokens`: The maximum number of tokens to be generated in the model's responses.
- `temperature`: The likelihood of the model selecting higher-probability options while generating a response.
- `top_p`: The percentage of most-likely candidates that the model considers for the next token.
- `stop`: A list of tokens that the model should stop generating at.

<Tip>
For the `default` and `conversational` modes, one of the following need to be provided:
    * `prompt_template`.
    * `question_column`, and an optional `context_column`.
</Tip>

## Usage

### Default Mode

In the `default` mode, the model will generate a separate response for each input provided. No context is maintained between the inputs.

```sql
CREATE MODEL bedrock_default_model
PREDICT answer
USING
    engine = 'bedrock_engine',
    prompt_template = 'Answer the users input in a helpful way: {{question}}';
```

To generate a response for a single input, the following query can be used:

```sql
SELECT *
FROM bedrock_default_model
WHERE question = 'What is the capital of Sweden?';
```

The response will look like the following:

| question                       | answer                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| What is the capital of Sweden? | The capital of Sweden is Stockholm. Stockholm is the largest city in Sweden, with a population of over 900,000 people in the city proper and over 2 million in the metropolitan area. It is known for its beautiful architecture, scenic waterways, and rich cultural heritage. The city is built on 14 islands, which are connected by over 50 bridges, and is home to many museums, galleries, and historic landmarks. Some of the most famous attractions in Stockholm include the Vasa Museum, the Stockholm Palace, and the Old Town (Gamla Stan). |

To generate responses for multiple inputs, the following query can be used:

```sql
SELECT *
FROM files.unrelated_questions AS d
JOIN bedrock_default_model AS m
```

The response will look like the following:

| question                                       | answer                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| What is the capital of Sweden?                 | The capital of Sweden is Stockholm. Stockholm is the most populated city in Sweden with over 975,000 residents. The city is known for its stunning architecture and beautiful waterways.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| What is the second planet in the solar system? | The second planet from the sun in our solar system is Venus. Venus is often called Earth's "sister planet" because of their similar size, mass, and density. However, the two planets have very different atmospheres and surface conditions. Venus has a thick, toxic atmosphere composed of carbon dioxide, which traps heat and causes the planet to have surface temperatures that can reach up to 471 degrees Celsius (880 degrees Fahrenheit). Venus also has a highly reflective cloud cover that obscures its surface, making it difficult to study. Despite these challenges, Venus has been the subject of numerous scientific missions, including several orbiters and landers that have provided valuable insights into the planet's geology, atmosphere, and climate. |

<Tip>
`files.unrelated_questions` is a simple CSV file containing a `question` column (as expected by the above model) that has been uploaded to MindsDB. It is, however, possible to use any other supported data source in the same manner.
</Tip>

### Conversational Mode

In the `conversational` mode, the model will maintain context between the inputs and generate a single response. This response will be placed in the last row of the result set.

```sql
CREATE MODEL bedrock_conversational_model
PREDICT answer
USING
    engine = 'bedrock_engine',
    mode = 'conversational',
    question_column = 'question';
```

The syntax for generating responses in the `conversational` mode is the same as in the `default` mode.

However, when generating responses for multiple inputs, the difference between the two modes becomes apparent. As mentioned above, the `conversational` mode maintains context between the inputs and generates a single response, which is placed in the last row of the result set:

```sql
SELECT *
FROM files.related_questions AS d
JOIN bedrock_default_model AS m
```

This is what the response will look like:

| question                                  | answer                                                                                                                                                                                                                                                                                                                                                                                                               |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| What is the capital of Sweden?            | [NULL]                                                                                                                                                                                                                                                                                                                                                                                                               |
| What are some cool places to visit there? | The capital of Sweden is Stockholm. It’s a beautiful city, with lots of old buildings and a scenic waterfront. You should definitely visit the Royal Palace, which is the largest palace in Scandinavia. You can also visit the Vasa Museum, which has a famous 17th-century warship that sank in Stockholm harbor. And you should definitely check out the ABBA Museum, which is dedicated to the famous pop group. |
