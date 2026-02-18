# Rocket Chat API Handler

This handler integrates with the [Rocket Chat API](https://developer.rocket.chat/reference/api) to read and write messages.

### Connect to the Rocket Chat API
We start by creating a database to connect to the Rocket Chat API.

```
CREATE DATABASE my_rocket_chat
WITH
  ENGINE = "rocket_chat"
  PARAMETERS = {
    "username": <username or email>,
    "password": <password>,
    "domain": <rocket chat domain (e.g. https://mindsdb.rocket.chat)>
  };
```

### Select Data
To see if the connection was successful, try searching for messages in a channel

```
SELECT *
FROM my_rocket_chat.channel_messages WHERE room_id="GENERAL";
```

Each row should look like this:

| id | room_id | bot_id | text | username | name | sent_at | 
| ----------- | -----------  | ----------- | ----------- | ----------- | ----------- | ----------- |
| PbrLoFpxYk2bbkvyA | GENERAL | [NULL] | Sample message | minds.db | MindsDB  | 2023-05-05T16:41:57.998Z |

where:
* id - ID of the message
* room_id - ID of the channel/room the message was sent in
* bot_id - ID of the bot that sent this message if applicable
* text - Actual message text
* username - Username for sent message
* name: Full name for sent message
* sent_at: When the message was sent in 'YYYY-MM-DDTHH:MM:SS.mmmZ' format


## Posting Messages

You can also post messages to a Rocket Chat channel using MindsDB:

```
INSERT INTO my_rocket_chat.channel_messages (room_id, text) VALUES ("GENERAL", "This is a test message!")
```

Supported insert columns:
* room_id (REQUIRED) - ID of room to send message to
* text (REQUIRED) - Message to send
* alias - What the message's name will appear as (username will still display).
* emoji - Sets the avatar on the message to be this emoji (e.g. :smirk:).
* avatar - Image URL to use for the message avatar.