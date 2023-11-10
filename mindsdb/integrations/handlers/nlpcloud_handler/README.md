# NLPCloud Handler

NLPCloud handler for MindsDB provides interfaces to connect to NLPCloud via APIs and pull repository data into MindsDB.

## About NLPCloud

NLP Cloud is an artificial intelligence platform that allows you to use the most advanced AI engines, and even train your own engines with your own data. This platform is focused on data privacy by design so you can safely use AI in your business without compromising confidentiality. We offer both small specific AI engines and large cutting-edge generative AI engines so you can easily integrate the most advanced AI features into your application at an affordable cost. NLPCloud API can be found [here](https://docs.nlpcloud.com/#endpoints)

## Implemented Features

- [x] NLPCloud ML Handler
  - [x] Translation
  - [x] Summarization
  - [x] Sentiment Analysis
  - [x] Paraphrasing
  - [x] Language Detection
  - [x] Named Entity Recognition

## Example Usage

The first step is to create a ML Engine with the new `nlpcloud` engine.

~~~~sql
CREATE ML_ENGINE nlpcloud_engine
FROM nlpcloud
USING
  api_key = 'your_api_key';
~~~~


The next step is to create a model with a `task` that signifies what type of transformation or generation is needed. The supported values for `task` are as follows:

- translation
- summarization
- sentiment
- paraphrasing
- langdetection
- ner


## Create a model

~~~~sql
CREATE MODEL mindsdb.nlpcloud_translation
PREDICT image
USING
  engine = "nlpcloud_engine",
  task = "translation",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.nlpcloud_summ
PREDICT image
USING
  engine = "nlpcloud_engine",
  task = "summarization",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.nlpcloud_sent
PREDICT image
USING
  engine = "nlpcloud_engine",
  task = "sentiment",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.nlpcloud_para
PREDICT image
USING
  engine = "nlpcloud_engine",
  task = "paraphrasing",
  api_key = "api_key"
~~~~

~~~~sql
CREATE MODEL mindsdb.nlpcloud_langdet
PREDICT image
USING
  engine = "nlpcloud_engine",
  task = "langdetection",
  api_key = "api_key"
~~~~

`task` and `api_key` are mandatory parameters for creating a model.

## Use the model

~~~~sql
SELECT *
FROM mindsdb.nlpcloud_translation
WHERE text = "" AND source_lang = "" AND target_lang = "";
~~~~

~~~~sql
SELECT *
FROM mindsdb.nlpcloud_translation
WHERE text = "" AND source_lang = "" AND target_lang = "";
~~~~

~~~~sql
SELECT *
FROM mindsdb.nlpcloud_translation
WHERE text = "" AND source_lang = "" AND target_lang = "";
~~~~

~~~~sql
SELECT *
FROM mindsdb.nlpcloud_translation
WHERE text = "" AND source_lang = "" AND target_lang = "";
~~~~