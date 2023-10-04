# Cohere Handler

Cohere ML handler for MindsDB provides interfaces to connect with Cohere ML via APIs and pull Cohere ML Capabilites into MindsDB.

## Cohere

Cohere platform provides solutions to many natural language use cases, including classification, semantic search, paraphrasing, summarization, and content generation. 
In this handler,python client of cohere api is used and more information about this python client can be found (here)[https://docs.cohere.com/reference/about]


## Implemented Features

- [x] Cohere ML Handler
  - [x] Support Generate
  - [x] Support Detect_language 
  - [x] Support Summarize

## Example Usage

The first step is to create a ML Engine with the new `cohere` engine.

~~~~sql
CREATE ML_ENGINE Cohere_ML_Engine
FROM cohere
USING
  api_key = 'your_api_key';
~~~~

### Detect_language 
Create a model:
~~~~sql
CREATE MODEL mindsdb.cohere_language_detector
PREDICT language
USING
  task = 'language-detection',
  column = 'text',
  engine = 'cohere',
  api_key = 'your_api_key'
~~~~
Query the model:
~~~~sql
SELECT text, language
FROM mindsdb.cohere_language_detector
WHERE text = 'Здравствуй, Мир';
~~~~


### Summarize

~~~~sql
CREATE MODEL mindsdb.cohere_text_summarization
PREDICT summary
USING
  task = 'text-summarization',
  column = 'text',
  engine = 'cohere',
  api_key = 'your_api_key'

SELECT text, summary
FROM mindsdb.cohere_text_summarization
WHERE text = 'The tower is 330 metres (1,083 ft) tall,[6] about the same height as an 81-storey building, and the tallest structure in Paris. Its base is square, measuring 125 metres (410 ft) on each side. During its construction, the Eiffel Tower surpassed the Washington Monument to become the tallest human-made structure in the world, a title it held for 41 years until the Chrysler Building in New York City was finished in 1930. It was the first structure in the world to surpass both the 200-metre and 300-metre mark in height. Due to the addition of a broadcasting aerial at the top of the tower in 1957, it is now taller than the Chrysler Building by 5.2 metres (17 ft). Excluding transmitters, the Eiffel Tower is the second tallest free-standing structure in France after the Millau Viaduct. The tower has three levels for visitors, with restaurants on the first and second levels.He decorated it with furniture by Jean Lachaise and invited friends such as Thomas Edison.';
~~~~

### Generate

~~~~sql
CREATE MODEL mindsdb.cohere_text_generation
PREDICT next_text
USING
  task = 'text-generation',
  column = 'text',
  engine = 'cohere',
  api_key = 'your_api_key'

SELECT text, next_text
FROM mindsdb.cohere_text_generation
WHERE text = 'Once upon on a time'
~~~~
