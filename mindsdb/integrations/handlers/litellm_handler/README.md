# Litellm Handler

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?

litellm (https://docs.litellm.ai/docs/) is a library that allows you to use popular LLM models such as OpenAI's GPT-3 and Anthropic claude-2. 
This handler allows you to use litellm models in MindsDB.

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

It provides a simple interface to use LLM models in MindsDB, see https://docs.litellm.ai/docs/providers for a list of supported providers.

## Are models created with this integration fast and scalable, in general?

Yes, the models are hosted by the provider, so there is no need for any additional system specifications. The only real requirement is to have an internet connection.

## What are the recommended system specifications for models created with this framework?

Since we use hosted LLM APIs, there is no need for any additional system specifications. The only real requirement is to have an internet connection.

## To what degree can users control the underlying framework by passing parameters via the USING syntax?

Users are allowed complete control over the underlying framework by passing parameters via the USING syntax.

## Does this integration offer model explainability or insights via the DESCRIBE syntax?

No, this integration doesn't support DESCRIBE syntax.

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?

No, fine-tuning is not supported.

## Are there any other noteworthy aspects to this handler?

The handler has a number of default parameters set, the user only needs to pass (see settings.py for more details):

- model: str - the name of the model to use
- api_key: str - the api key for the provider of the model

## Any directions for future work in subsequent versions of the handler?

tbc


## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)

```sql
-- Create LLM engine NB you can provide api keys here or at create
-- You only need to provide key of llm you intend to use

create ML_Engine litellm from litellm;

--simple completion with openai

create model litellm_handler_simple
predict text
using
engine="litellm",
model="gpt-3.5-turbo",
api_key="openai-api-key";

select * from information_schema.models where name ="litellm_handler_simple" ;

select * from litellm_handler_simple where text='Once upon a time';

--simple completion with anthropic

create model litellm_handler_simple_anthropic
predict text
using
engine="litellm",
model="claude-2",
api_key="anthropic-api-key";

select * from information_schema.models where name ="litellm_handler_simple_anthropic" ;

select * from litellm_handler_simple_anthropic where text='Once upon a time';

--completion using prompt_template on create

create model litellm_handler_prompt_template
predict text
using
engine="litellm",
model="gpt-3.5-turbo",
api_key="openai-api-key",
prompt_template="write a story based on {{text}}";

select * from information_schema.models where name ="litellm_handler_prompt_template" ;

select * from litellm_handler_prompt_template where text='Once upon a time';

--completion using prompt_template on predict

create model litellm_handler_prompt_template_predict
predict text
using
engine="litellm",
model="gpt-3.5-turbo",
api_key="openai-api-key";

select * from information_schema.models where name ="litellm_handler_prompt_template_predict" ;

select * from litellm_handler_prompt_template_predict 
where text='Once upon a time' prompt_template="write a story based on {{text}}";


--completion using messages with batch_completion (multiple messages)

create model litellm_handler_messages
predict text
using
engine="litellm",
model="gpt-3.5-turbo",
api_key="openai-api-key";
       
select * from information_schema.models where name ="litellm_handler_messages" ;

select * from litellm_handler_messages 
where messages='[[{"content": "The sky is blue", "role": "user"}], [{"content": "The grass is green", "role": "user"}]]';
```
