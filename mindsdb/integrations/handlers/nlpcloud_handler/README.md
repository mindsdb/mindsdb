# NLPCloud Handler

NLPCloud handler for MindsDB provides interfaces to connect to NLPCloud via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [NLPCloud Handler](#nlpcloud-handler)
  - [Table of Contents](#table-of-contents)
  - [About NLPCloud](#about-nlpcloud)
  - [NLPCloud Handler Implementation](#nlpcloud-handler-implementation)
  - [NLPCloud Handler Initialization](#nlpcloud-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About NLPCloud

NLP Cloud is an artificial intelligence platform that allows you to use the most advanced AI engines, and even train your own engines with your own data. This platform is focused on data privacy by design so you can safely use AI in your business without compromising confidentiality. We offer both small specific AI engines and large cutting-edge generative AI engines so you can easily integrate the most advanced AI features into your application at an affordable cost.


## NLPCloud Handler Implementation

This handler was implemented using the `nlpcloud` library that makes http calls to https://docs.nlpcloud.com/#endpoints

## NLPCloud Handler Initialization

The NLPCloud handler is initialized with the following parameters:

- `token`: Auth token to connect with NLP Cloud
- `model`: Machine Learning Model
- `gpu`: Whether to use gpu or not. It's a boolean value.
- `lang`: Language

Read about creating an account [here](https://nlpcloud.com/).
The list of models supported by nlpcloud can be found [here](https://docs.nlpcloud.com/#models).

## Implemented Features

- [x] NLPCloud
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Code generation
- [ ] Automatic Speech Recognition
- [ ] Classification
- [ ] Generation

etc.. Find the list [here](https://docs.nlpcloud.com/#endpoints)

## Example Usage

The first step is to create a database with the new `nlpcloud` engine. 

~~~~sql
CREATE DATABASE mindsdb_nlpcloud
WITH ENGINE = 'nlpcloud',
PARAMETERS = {
  "token": "",
  "model": "en_core_web_lg"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_nlpcloud.translation where text="hello mindsdb! How are ya?" AND source="eng_Latn" AND target="fra_Latn";
~~~~

~~~~sql
SELECT * FROM mindsdb_nlpcloud.summarization where text="One month after the United States began what has become a troubled rollout of a national COVID vaccination campaign, the effort is finally gathering real steam. Close to a million doses -- over 951,000, to be more exact -- made their way into the arms of Americans in the past 24 hours, the U.S. Centers  for Disease Control and Prevention reported Wednesday. That s the largest number of shots given in one day since the rollout began and a big jump from the previous day, when just under 340,000 doses were given, CBS News reported. That number is likely to jump quickly after the federal government on Tuesday gave states the OK to vaccinate anyone over 65 and said it would release all the doses of vaccine it has available for distribution. Meanwhile, a number of states have now opened mass vaccination sites in an effort to get larger numbers of people inoculated, CBS News reported.";
~~~~

~~~~sql
SELECT * FROM mindsdb_nlpcloud.sentiment_analysis where text="Mindsdb is an excellent product";
~~~~

~~~~sql
SELECT * FROM mindsdb_nlpcloud.language_detection where text="你好你好吗";
~~~~

~~~~sql
SELECT * FROM mindsdb_nlpcloud.named_entity_recognition where text="The World Health Organization (WHO)[1] is a specialized agency of the United Nations responsible for international public health.[2] The WHO Constitution states its main objective as 'the attainment by all peoples of the highest possible level of health'.[3] Headquartered in Geneva, Switzerland, it has six regional offices and 150 field offices worldwide.";
~~~~