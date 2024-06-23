# Slack Handler

Slack Handler for MindsDB provides interfaces to connect with Slack via APIs and perform various tasks such as sending messages, retrieving conversation history, and more.

## Slack

Slack is a popular communication platform widely used in organizations. MindsDB Slack handler can be used to integrate Slack with MindsDB and automate various tasks.

## Slack Handler Implementation

This handler was implemented using [slack-sdk](https://slack.dev/python-slack-sdk/) library.
slack-sdk is a Python Library which offers a corresponding package for each of Slack’s APIs

## Slack Handler Initialization

The Slack Handler is initialized with the following parameters:

- `token`: Slack API token to use for authentication

To Generate a Slack API Token, follow these steps:

1. Follow this [Link](https://api.slack.com/apps) and sign in with your Slack Account
2. Create a new App/Bot or select an existing App.
3. In the app settings, go to the "OAuth & Permissions" section.
4. Under the "Bot Token Scopes" section, add the necessary scopes for your application. You can add more later as well.
5. Install the bot to your workspace.
6. In the "OAuth Tokens & Redirect URLs" Section, copy the the Bot User OAuth Access Token.
7. Open your Slack Application, in order to use the bot which we created, we have to add the bot into the channel where we want to use this.
    - Go to any the channel where you want to use the bot.
    - Right Click on the channel and select 'View Channel Details'.
    - Select 'Integrations'.
    - Click on 'Add an App'.
    - You can see the name of the bot under the 'In your workspace' Section, Go ahead and add the app to the channel.

Now, You can use the token from step 6 to initialize the Slack Handler in MindsDB

## Implemented Features

**Slack channels Table**
   - [x] Supports SELECT
   - [x] Supports DELETE
   - [x] Supports WHERE
   - [x] Supports ORDER BY
   - [x] Supports LIMIT
   - [x] Integrate with OpenAI Integration

## Example Usage

Creates a database with the `slack` engine

~~~~sql
CREATE DATABASE mindsdb_slack
WITH
  ENGINE = 'slack',
  PARAMETERS = {
      "token": "<slack-bot-token>"
    };
~~~~

Please change the `slack-bot-token` with the token mentioned in `Bot User OAuth Access Token`.

List of channels:

~~~~sql
SELECT * FROM mindsdb_slack.channel_lists;
~~~~

Retrieve the Conversation from a specific Slack channel

~~~~sql
SELECT *
FROM mindsdb_slack.channels
WHERE channel="<channel-name>";
~~~~

Please change the `channel-name` in the `WHERE` clause to the channel where, you added the bot in your Slack Workspace.

Post a new message to a Channel

~~~~sql
INSERT INTO mindsdb_slack.channels (channel, message)
VALUES("<channel-name>", "Hey MindsDB, Thanks to you! Now I can respond to my Slack messages through SQL Queries. 🚀 ");
~~~~

Whoops, Sent it by mistake, No worries, use this to delete a specific message

~~~~sql
DELETE FROM mindsdb_slack.channels
WHERE channel = "<channel-name>" AND ts = "1688863707.197229";
~~~~

Updating a message in channel change the `channel-name` and `timestamp` in the `WHERE` clause
~~~~sql
UPDATE mindsdb_slack.channels
SET text = 'sample message is updated.'
WHERE channel = "<channel-name>" AND ts = '<timestamp>';
~~~~

Selects only 10 created after the specified timestamp

~~~~sql
SELECT *
FROM mindsdb_slack.channels
WHERE channel="<channel-name>" AND created_at > '2023-07-25 00:13:07'
LIMIT 10;
~~~~

Retrieves 5 latest messages in Ascending order

~~~~sql
SELECT *
FROM mindsdb_slack.channels
WHERE channel="<channel-name>"
ORDER BY messages ASC
LIMIT 5;
~~~~

## Threads Table
Retrieve the Conversation Replies from a specific Slack message

~~~~sql
SELECT *
FROM mindsdb_slack.threads
WHERE channel="<channel-name>"
AND ts='<timestamp>';
~~~~

Please change the `channel-name` and `timestamp` in the `WHERE` clause to the channel and ts of messages of whic you want to see replies.

Post a new reply to a message

~~~~sql
INSERT INTO mindsdb_slack.threads (channel, ts, message)
VALUES("<channel-name>", "<timestamp>", "This is my Answer, Just wait and Watch. 🚀 ");
~~~~

Whoops, Sent it by mistake, No worries, use this to delete a specific thread/reply

~~~~sql
DELETE FROM mindsdb_slack.threads
WHERE channel = "<channel-name>" AND ts = "1688863707.197229";
~~~~

Updating a reply in channel change the `channel-name` and `timestamp` in the `WHERE` clause
~~~~sql
UPDATE mindsdb_slack.threads
SET text = 'sample message is updated.'
WHERE channel = "<channel-name>" AND ts = '<timestamp>';
~~~~


## Let's Use GPT Model to respond to messages for us
Let's first create a GPT model that can respond to messages asked by the users. We will create a model that accepts prompt based on the prompt, the model will respond to the messages.

~~~~sql
CREATE MODEL mindsdb.slack_response_model
PREDICT response
USING
engine = 'openai',
max_tokens = 300,
api_key = '<your-api-token>',
model_name = 'gpt-3.5-turbo',
prompt_template = 'From input message: {{messages}}\
write a short response to the user in the following format:\
Hi, I am an automated bot here to help you, Can you please elaborate the issue which you are facing! ✨🚀 -- mdb.ai/bot by @mindsdb';
~~~~

We can ask basic questions to the bot about the project, it will respond with the appropriate answer.

~~~~sql
SELECT
  messages, response
FROM mindsdb.slack_response_model
WHERE
  messages = 'Hi, can you please explain me more about MindsDB?';
~~~~

Let's now work with the real-time questions asked by the users in the slack channel. This query returns output generated by the GPT model for the queries asked in the slack channel.

~~~~sql
SELECT
    t.channel as channel,
    t.messages as input_text, 
    r.response as message
FROM mindsdb_slack.channels as t
JOIN mindsdb.slack_response_model as r
WHERE t.channel = '<channel-name>'
LIMIT 3;
~~~~

To Post the Response from the GPT model to the slack channel.

~~~~sql
INSERT INTO mindsdb_slack.channels(channel, message)
  SELECT
    t.channel as channel,
    t.messages as input_text, 
    r.response as message
  FROM mindsdb_slack.channels as t
  JOIN mindsdb.slack_response_model as r
  WHERE t.channel = '<channel-name>'
  LIMIT 3;
~~~~

## Schedule a Job

Finally, we can let GPT model respond to all the questions asked by the users by scheduling a Job where:
- GPT model will check for new messages
- Generate an appropriate response
- Posts the message into the Channel

~~~~sql
CREATE JOB mindsdb.gpt4_slack_job AS (
   -- insert into channels the output of joining model and new responses
  INSERT INTO mindsdb_slack.channels(channel, message)
  SELECT
    t.channel as channel,
    t.messages as input_text, 
    r.response as message
  FROM mindsdb_slack.channels as t
  JOIN mindsdb.slack_response_model as r
  WHERE t.channel = '<channel-name>' AND t.created_at > "2023-07-25 05:22:00" AND t.created_at > "{{PREVIOUS_START_DATETIME}}"
  LIMIT 3;
)
EVERY minute;
~~~~

Every minute, our model is going to look for the questions, and the model will generate the response and send the message to the channel. Model can send only three messages every minute.

## What's Next?

- Add functionality to Update messages
  