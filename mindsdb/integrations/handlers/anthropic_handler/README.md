# Anthropic Handler
The Anthropic ML handler integrates Anthropic LLMs with MindsDB. You can use it to generate text completions with the popular Claude LLM family for your existing text data.

# Anthropic
Anthropic is an AI safety and research company. Claude is their assistant model.The package used is the Anthropic Python SDK.More information about this Python client can be found [here](https://github.com/anthropics/anthropic-sdk-python).

*Note:* For more information on evaluation terms, read [here](https://docs.anthropic.com/claude/reference/getting-started-with-the-api#evaluation--going-live-with-the-api)


# Example Usage

Create a ML Engine with the new `anthropic` engine.

~~~~sql
CREATE ML_ENGINE Anthropic_ML_Engine
FROM anthropic
USING
  api_key = 'your_api_key';
~~~~

~~~ sql
CREATE MODEL mindsdb.anthropic_test
PREDICT answer
USING
  column = 'question',
  engine = 'anthropic',
  api_key = 'your_api_key',
  max_tokens = '300',
  model = 'claude-2'
~~~

~~~ sql
SELECT question, answer
FROM mindsdb.anthropic_test
WHERE question = 'How to solve the climate change crisis?';
~~~

~~~ sql
SELECT t.question, m.answer
FROM mindsdb.anthropic_test as m
JOIN files.question_table as t;
~~~~
