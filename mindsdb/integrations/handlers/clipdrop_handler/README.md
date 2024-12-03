# Clipdrop Handler

Clipdrop handler for MindsDB provides interfaces to connect to Clipdrop via APIs.

## About Clipdrop

The Clipdrop API allows you to integrate best-in-class AI to your apps in minutes.

## Implemented Features

- [x] Clipdrop ML Handler
  - [x] Remove Text from Image
  - [x] Remove Background from Image
  - [x] Generate Image from sketch
  - [x] Generate Image from text
  - [x] Reimagine the Image
  - [x] Replace Background in Image

## Example Usage

The first step is to create a ML Engine with the new `clipdrop` engine.

~~~~sql
CREATE ML_ENGINE clipdrop_engine
FROM clipdrop
USING
  clipdrop_api_key = 'your_api_key';
~~~~


The next step is to create a model with a `task` that signifies what type of transformation or generation is needed. The supported values for `task` are as follows:

- [remove_text](#remove-text-from-image)
- [remove_background](#remove-background-from-image)
- [sketch_to_image](#generate-image-from-sketch)
- [text_to_image](#generate-image-from-text)
- [reimagine](#re-imagine-the-image)
- [replace_background](#replace-background-in-image)


## Create a model

### Remove Text from Image

~~~~sql
CREATE MODEL mindsdb.clipdrop_rt
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "remove_text",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_rt
WHERE image_url = "https://onlinejpgtools.com/images/examples-onlinejpgtools/calm-body-of-water-with-quote.jpg";
~~~~

### Remove Background from Image

~~~~sql
CREATE MODEL mindsdb.clipdrop_rb
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "remove_background",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_rb
WHERE image_url = "https://static.clipdrop.co/web/apis/remove-background/input.jpg";
~~~~

### Generate Image from Sketch

~~~~sql
CREATE MODEL mindsdb.clipdrop_s2i
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "sketch_to_image",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_s2i
WHERE image_url = 'https://img.freepik.com/free-vector/hand-drawn-cat-outline-illustration_23-2149266368.jpg'
  AND text = 'brown cat';
~~~~

### Generate Image from Text

~~~~sql
CREATE MODEL mindsdb.clipdrop_t2i
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "text_to_image",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_t2i
WHERE text = 'Imagine a software engineer';
~~~~

### Re-imagine the Image

~~~~sql
CREATE MODEL mindsdb.clipdrop_reimagine
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "reimagine",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_reimagine
WHERE image_url = "https://static.clipdrop.co/web/apis/remove-background/input.jpg";
~~~~

### Replace Background in Image

~~~~sql
CREATE MODEL mindsdb.clipdrop_rbi
PREDICT image
USING
  engine = "clipdrop_engine",
  task = "replace_background",
  local_directory_path = "/Users/Sam/Downloads/test";
~~~~

~~~~sql
SELECT *
FROM mindsdb.clipdrop_rbi
WHERE image_url = "https://static.clipdrop.co/web/apis/remove-background/input.jpg"
  AND text = "Empty road";
~~~~

**Note:** `task`, `local_directory_path` and `clipdrop_api_key` are mandatory parameters for creating a model. The `clipdrop_api_key` should be provided when createing an ML_ENGINE.