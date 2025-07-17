---
title: Slack
sidebarTitle: Slack
---

This documentation describes the integration of MindsDB with [Slack](https://slack.com/), a cloud-based collaboration platform.
The integration allows MindsDB to access data from Slack and enhance Slack with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Slack to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Install or ensure access to Slack.

## Connection

Establish a connection to Slack from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/slack_handler) as an engine.

```sql
CREATE DATABASE slack_datasource
WITH ENGINE = 'slack', 
PARAMETERS = {
   "token": "values",     -- required parameter
   "app_token": "values"  -- optional parameter
};
```

The Slack handler is initialized with the following parameters:

* `token` is a Slack bot token to use for authentication.
* `app_token` is a Slack app token to use for authentication.

<Note>
Please note that `app_token` is an optional parameter. Without providing it, you need to integrate an app into a Slack channel.
</Note>

### Method 1: Chatbot responds in direct messages to a Slack app

One way to connect Slack is to use both bot and app tokens. By following the instructions below, you'll set up the Slack app and be able to message this Slack app directly to chat with the bot.

<Note>
If you want to use Slack in the [`CREATE CHATBOT`](/agents/chatbot) syntax, use this method of connecting Slack to MindsDB.
</Note>

<Accordion title="Set up a Slack app and generate tokens">
Here is how to set up a Slack app and generate both a Slack bot token and a Slack app token:

  1. Follow [this link](https://api.slack.com/apps) and sign in with your Slack account.
  2. Create a new app `From scratch` or select an existing app.
      - Please note that the following instructions support apps created `From scratch`.
      - For apps created `From an app manifest`, please follow the [Slack docs here](https://api.slack.com/reference/manifests).
  3. Go to *Basic Information* under *Settings*.
      - Under *App-Level Tokens*, click on *Generate Token and Scopes*.
      - Name the token `socket` and add the `connections:write` scope.
      - **Copy and save the `xapp-...` token - you'll need it to publish the chatbot.**
  4. Go to *Socket Mode* under *Settings* and toggle the button to *Enable Socket Mode*.
  5. Go to *OAuth & Permissions* under *Features*.
      - Add the following *Bot Token Scopes*:
        - app_mentions:read
        - channels:history
        - channels:read
        - chat:write
        - groups:history
        - groups:read (optional)
        - im:history
        - im:read
        - im:write
        - mpim:read (optional)
        - users.profile:read
        - users:read (optional)
      - In the *OAuth Tokens for Your Workspace* section, click on *Install to Workspace* and then *Allow*.
      - **Copy and save the `xoxb-...` token - you'll need it to publish the chatbot.**
  6. Go to *App Home* under *Features* and click on the checkbox to *Allow users to send Slash commands and messages from the messages tab*.
  7. Go to *Event Subscriptions* under *Features*.
      - Toggle the button to *Enable Events*.
      - Under *Subscribe to bot events*, click on *Add Bot User Event* and add `app_mention` and `message.im`.
      - Click on *Save Changes*.
  8. Now you can use tokens from points 3 and 5 to initialize the Slack handler in MindsDB.
</Accordion>

<Note>
This connection method enables you to chat directly with an app via Slack.

Alternatively, you can connect an app to the Slack channel:
  - Go to the channel where you want to use the bot.
  - Right-click on the channel and select *View Channel Details*.
  - Select *Integrations*.
  - Click on *Add an App*.
</Note>

Here is how to connect Slack to MindsDB:

```sql
CREATE DATABASE slack_datasource
WITH
  ENGINE = 'slack',
  PARAMETERS = {
      "token": "xoxb-...",
      "app_token": "xapp-..."
    };
```

It comes with the `conversations` and `messages` tables.

### Method 2: Chatbot responds on a defined Slack channel

Another way to connect to Slack is to use the bot token only. By following the instructions below, you'll set up the Slack app and integrate it into one of the channels from which you can directly chat with the bot.

<Accordion title="Set up a Slack app and generate tokens">
Here is how to set up a Slack app and generate a Slack bot token:

  1. Follow [this link](https://api.slack.com/apps) and sign in with your Slack account.
  2. Create a new app `From scratch` or select an existing app.
      - Please note that the following instructions support apps created `From scratch`.
      - For apps created `From an app manifest`, please follow the [Slack docs here](https://api.slack.com/reference/manifests).
  3. Go to the *OAuth & Permissions* section.
  4. Under the *Scopes* section, add the *Bot Token Scopes* necessary for your application. You can add more later as well.
      - channels:history
      - channels:read
      - chat:write
      - groups:read
      - im:read
      - mpim:read
      - users:read
  5. Install the bot in your workspace.
  6. Under the *OAuth Tokens for Your Workspace* section, copy the the *Bot User OAuth Token* value.
  7. Open your Slack application and add the App/Bot to one of the channels:
      - Go to the channel where you want to use the bot.
      - Right-click on the channel and select *View Channel Details*.
      - Select *Integrations*.
      - Click on *Add an App*.
  8. Now you can use the token from step 6 to initialize the Slack handler in MindsDB and use the channel name to query and write messages.
</Accordion>

Here is how to connect Slack to MindsDB:

```sql
CREATE DATABASE slack_datasource
WITH
  ENGINE = 'slack',
  PARAMETERS = {
      "token": "xoxb-..."
    };
```

## Usage

<Warning>
The following usage applies when **Connection Method 2** was used to connect Slack.

See the usage for **Connection Method 1** [via the `CREATE CHATBOT` syntax](/sql/tutorials/create-chatbot).
</Warning>

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM slack_datasource.table_name
LIMIT 10;
```

## Supported Tables

The Slack integration supports the following tables:

### `conversations` Table

The `conversations` virtual table is used to query conversations (channels, DMs, and groups) in the connected Slack workspace.

```sql
-- Retrieve all conversations in the workspace
SELECT * 
FROM slack_datasource.conversations;

-- Retrieve a specific conversation using its ID
SELECT * 
FROM slack_datasource.conversations 
WHERE id = "<channel-id>";

-- Retrieve a specific conversation using its name
SELECT *
FROM slack_datasource.conversations
WHERE name = "<channel-name>";
```

### `messages` Table

The `messages` virtual table is used to query, post, update, and delete messages in specific conversations within the connected Slack workspace.

```sql
-- Retrieve all messages from a specific conversation
-- channel_id is a required parameter and can be found in the conversations table
SELECT * 
FROM slack_datasource.messages 
WHERE channel_id = "<channel-id>";

-- Post a new message
-- channel_id and text are required parameters
INSERT INTO slack_datasource.messages (channel_id, text)
VALUES("<channel-id>", "Hello from SQL!");

-- Update a bot-posted message
-- channel_id, ts, and text are required parameters
UPDATE slack_datasource.messages
SET text = "Updated message content"
WHERE channel_id = "<channel-id>" AND ts = "<timestamp>";

-- Delete a bot-posted message
-- channel_id and ts are required parameters
DELETE FROM slack_datasource.messages
WHERE channel_id = "<channel-id>" AND ts = "<timestamp>";
```

<Tip>
You can also find the channel ID by right-clicking on the conversation in Slack, selecting 'View conversation details' or 'View channel details,' and copying the channel ID from the bottom of the 'About' tab.
</Tip>

### `threads` Table

The `threads` virtual table is used to query and post messages in threads within the connected Slack workspace.

```sql
-- Retrieve all messages in a specific thread
-- channel_id and thread_ts are required parameters
-- thread_ts is the timestamp of the parent message and can be found in the messages table
SELECT * 
FROM slack_datasource.threads 
WHERE channel_id = "<channel-id>" AND thread_ts = "<thread-ts>";

-- Post a message to a thread
INSERT INTO slack_datasource.threads (channel_id, thread_ts, text)
VALUES("<channel-id>", "<thread-ts>", "Replying to the thread!");
```

### `users` Table

The `users` virtual table is used to query user information in the connected Slack workspace.

```sql
-- Retrieve all users in the workspace
SELECT * 
FROM slack_datasource.users;

-- Retrieve a specific user by name
SELECT * 
FROM slack_datasource.users 
WHERE name = "John Doe";
```

## Rate Limit Considerations

The Slack API enforces rate limits on data retrieval. Therefore, when querying the above tables, by default, the first 1000 (999 for `messages`) records are returned.

To retrieve more records, use the `LIMIT` clause in your SQL queries. For example:

```sql
SELECT *
FROM slack_datasource.conversations
LIMIT 2000;
```

When using the LIMIT clause to query additional records, you may encounter Slack API rate limits.

## Next Steps

Follow [this tutorial](use-cases/ai_agents/build_ai_agents) to build an AI agent with MindsDB.
