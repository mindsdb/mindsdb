# Leonardo AI Handler

Leonardo AI handler for MindsDB highlights key features such as automated content generation with Leonardo AI which promotes a cohesive workflow that combines the strengths of MindsDB.

## Leonardo AI

Leonardo AI is a generative AI tool that is renowned for creating AI art and generating image assets for various purposes, such as computer games, character design, concept art, graphic design, fashion, marketing, advertising, product photography, architecture, and interior design. It offers features like image generation, AI canvas, and text-to-image generation.

In this handler, API requests are used in order to perform the necessary operations, docs can be found [here](https://docs.leonardo.ai/reference/getuserself)

## Implemented Features

- [x] Leonardo AI Handler
  - [x] Supports Model Creation
  - [x] Supports Image Generate
  - [x] Supports DESCRIBE


## Example Usage

First step will be to create a ML Engine.

~~~~sql
CREATE ML_ENGINE leo_engine
FROM leonardo_ai
USING
    api_key = 'your_api_key';
~~~~

## Model Creation

Then we have to create a model with following parameters

~~~~sql
CREATE MODEL mindsdb.leo
PREDICT url
USING
   engine = 'leo_engine',
   model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3', -- default model
   -- api_key = 'your_api_key, either pass the api_key here or while engine creation
   -- height = 512, optional to pass height
   -- width = 768, optional to pass width
   prompt_template = '{{text}}, 8K | highly detailed realistic 3d oil painting style cyberpunk by MAD DOG JONES combined with Van Gogh  |  cinematic lighting | happy colors';
~~~~

## Image Generation

We can generate the images using the following query

~~~~sql
SELECT *
FROM mindsdb.leo
WHERE text = 'Generate a random ANIME picture'; -- please modify based on the need
~~~~

## Error Handling

To handler any errors caused by the model, run the following query

~~~~sql
DESCRIBE MODEL mindsdb.leo;
~~~~

OR to see the features, metadata of the model

~~~~sql
DESCRIBE MODEL mindsdb.leo.features;

DESCRIBE MODEL mindsdb.leo.metadata;
~~~~