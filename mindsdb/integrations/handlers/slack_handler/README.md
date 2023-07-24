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
- Go to Slack App Management Dashboard
- Create a new App or select an existing App.
- In the app settings, go to the "OAuth & Permissions" section.
- Under the "Bot Token Scopes" section, add the necessary scopes for you application. You can add more later as well.
- Install the app to your workspace.
- In the "OAuth Tokens & Redirect URLs" Section, copy the the Bot User OAuth Access Token.
- Go to your Slack App, in order to use the Slack App which we created, we have to add the bot into the channel where we want to use this bot.
    - Click on the channel in which you want to use the bot.
    - Go to 'View Channel Details'.
    - Select 'Integrations'.
    - Click on 'Add an App'.
    - You can see the name of the bot under the 'In your workspace' Section, Go ahead and add the app to the channel.

Now, You can use the token from the second last step to initialize the Slack Handler in MindsDB

## Implemented Features

- [x] Slack channels Table
    - [x] Support SELECT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support LIMIT

## Example Usage

Firstly, we have to create a database with the `slack` engine

~~~~sql
CREATE DATABASE mindsdb_slack
WITH
  ENGINE = 'slack',
  PARAMETERS = {
      "token": "<slack-bot-token>"
    };
~~~~

~~~~sql
SELECT messages
FROM slack_test.channels
WHERE channel="general";
~~~~

~~~~sql
SELECT messages
FROM slack_test.channels
WHERE channel="testing"
LIMIT 10;
~~~~

~~~~sql
SELECT messages
FROM slack_test.channels
WHERE channel="testing"
ORDER BY messages ASC
LIMIT 5;
~~~~

## What's Next?
- Add functionality to Update and Delete messages
- Experiment with SQL to use existing OpenAI and Hugging Face Integrations