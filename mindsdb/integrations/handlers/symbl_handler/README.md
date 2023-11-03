# Symbl Handler

Symbl handler for MindsDB provides interfaces to connect to Symbl via APIs and pull conversation data into MindsDB.

---

## Table of Contents

- [Symbl Handler](#symbl-handler)
  - [Table of Contents](#table-of-contents)
  - [About Symbl](#about-symbl)
  - [Symbl Handler Implementation](#symbl-handler-implementation)
  - [Symbl Handler Initialization](#symbl-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About Symbl

Transform unstructured audio, video and text data into structured insights, events and knowledge.


## Symbl Handler Implementation

This handler was implemented using the `symbl` library that makes http calls to https://docs.symbl.ai/docs/welcome.

## Symbl Handler Initialization

The Symbl handler is initialized with the following parameters:

- `app_id`: App Id
- `app_secret`: App Secret

Read about creating an account [here]().

## Implemented Features

- [x] Symbl Conversation
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## Example Usage

The first step is to create a database with the new `Symbl` engine. 

~~~~sql
CREATE DATABASE mindsdb_symbl
WITH ENGINE = 'symbl',
PARAMETERS = {
  "app_id": "app_id",
  "app_secret":"app_secret"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_symbl.get_conversation_id WHERE audio_url="https://symbltestdata.s3.us-east-2.amazonaws.com/newPhonecall.mp3";
~~~~

The above query may take longer to respond depending on the length of the audio.

~~~~sql
SELECT * FROM mindsdb_symbl.get_messages where conversation_id="5682305049034752";
~~~~

~~~~sql
SELECT * FROM mindsdb_symbl.get_topics where conversation_id="5682305049034752";
~~~~

~~~~sql
SELECT * FROM mindsdb_symbl.get_questions where conversation_id="5682305049034752";
~~~~

~~~~sql
SELECT * FROM mindsdb_symbl.get_analytics where conversation_id="5682305049034752";
~~~~

~~~~sql
SELECT * FROM mindsdb_symbl.get_action_items where conversation_id="5682305049034752";
~~~~

~~~~sql
SELECT * FROM mindsdb_symbl.get_follow_ups where conversation_id="5682305049034752";
~~~~

