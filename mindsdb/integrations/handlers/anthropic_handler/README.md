# Anthropic Handler
Anthropic ML handler integrates Anthropic LLMs with MindsDB. You can train your existing text data with Anthropic LLM

# Anthropic
Anthropic is a AI assitant framework for your usecases. In this handler, we use the Anthropic package, which is available in Python. More information about this Python client can be found (here)[https://github.com/anthropics/anthropic-sdk-python].


# Example Usage
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
