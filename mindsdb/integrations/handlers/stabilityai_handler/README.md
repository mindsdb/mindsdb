# Stability AI Handler

Stability AI ML handler for MindsDB provides interfaces to connect with Stability AI ML via APIs and pull Stability AI ML Capabilites into MindsDB.

## Stability AI

Stability AI world’s leading open source generative AI company. Learn more about stability AI[here](https://stability.ai/about)

## Implemented Features

- [x] Stability AI ML Handler
  - [x] Support Generate Image

## Example Usage

The first step is to create a ML Engine with the new `stabilityai` engine.

~~~~sql
CREATE ML_ENGINE stabilityai_engine
FROM stabilityai
USING
  api_key = 'your_api_key';
~~~~

### Generate Image from Text

Create a model:

~~~~sql
CREATE MODEL mindsdb.stability_image_generation
PREDICT image
USING
  local_directory_path = "/Users/sam/Documents/test",
  input_column = 'text',
  engine = 'stabilityai_engine',
  api_key = 'your_api_key'
~~~~

Query the model:

~~~~sql
SELECT text, image
FROM mindsdb.stability_image_generation
WHERE text = 'A blue lake';
~~~~
