# Stability AI Handler

Stability AI ML handler for MindsDB provides interfaces to connect with Stability AI ML via APIs and pull Stability AI ML Capabilites into MindsDB.

## Stability AI

Stability AI world’s leading open source generative AI company. Learn more about stability AI[here](https://stability.ai/about)

## Implemented Features

- [x] Stability AI ML Handler
  - [x] Support Generate Image from text
  - [x] Support Image to Image
  - [x] Support Image masking
  - [x] Support Image Upscaling

## TODO

- [ ] Clip Guidance
- [ ] Multiprompting

## Example Usage

The first step is to create a ML Engine with the new `stabilityai` engine.

~~~~sql
CREATE ML_ENGINE stabilityai_engine
FROM stabilityai
USING
  api_key = 'your_api_key';
~~~~


The next step is to create a model with a `task` that signifies what type of transformation or generation is needed. The supported values for `task` are as follows:

- text-to-image
- image-to-image
- image-upscaling
- image-masking


## Create a model

~~~~sql
CREATE MODEL mindsdb.stability_image_generation_t2i
PREDICT image
USING
  engine = "stabilityai_engine",
  task = "text-to-image",
  local_directory_path = "/Users/Sam/Downloads/test",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.stability_image_generation_i2i
PREDICT image
USING
  engine = "stabilityai_engine",
  task = "image-to-image",
  local_directory_path = "/Users/Sam/Downloads/test",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.stability_image_generation_mask
PREDICT image
USING
  engine = "stabilityai_engine",
  task = "image-masking",
  local_directory_path = "/Users/Sam/Downloads/test",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.stability_image_generation_upscale
PREDICT image
USING
  engine = "stabilityai_engine",
  task = "image-upscaling",
  local_directory_path = "/Users/Sam/Downloads/test",
  api_key = "api_key"
~~~~

`engine`, `task`, `local_directory_path` and `api_key` are mandatory parameters for creating a model.

## Use the model

~~~~sql
select * from mindsdb.stability_image_generation_t2i
where text = "A blue lake with fishes and birds flying over it";
~~~~

~~~~sql
select * from mindsdb.stability_image_generation_i2i
where image_url = "https://platform.stability.ai/TSgRPCImageToImageInit.png" and text = "crayon drawing of rocket ship launching from forest";
~~~~

~~~~sql
select * from mindsdb.stability_image_generation_mask
where image_url = "https://platform.stability.ai/Inpainting-C1.png" and mask_image_url = "https://platform.stability.ai/Inpainting-C2.png"
~~~~

~~~~sql
select * from mindsdb.stability_image_generation_upscale
where image_url = "https://platform.stability.ai/TSgRPCImageToImageInit.png" and height = 1500;
~~~~
