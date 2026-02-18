## About Discord

Discord is a communication platform designed around communities. It provides voice, video and text communication channels, along with various features for community management. See [discord.com](https://discord.com/) for more information.

# Discord Handler Setup

The Discord handler functions through a Discord bot, which must be registered on the [Discord Developer Portal](https://discord.com/developers). Make sure to give the bot the `Message Content` Privileged Intent the `Send Messages` permission in the channel you want to send messages to.

--- 

## Parameters

- `token`: a required token to give the bot access to the Discord API. This can be found on the Discord Developer Portal.

Step 1 of [this guide](https://discord.com/developers/docs/getting-started) covers the basics of provisioning a bot.

## Implemented Features

- [x] Send and receive messages to a Discord channel via the API

## TODO

- [ ] Support other options for sending messages such as embeds, images, etc.
- [ ] Support UPDATE and DELETE for messages table

## Example Usage

The first step is to create a database with the new `discord` engine by passing in the required `token` parameter:

~~~~sql
CREATE DATABASE discord_datasource
WITH ENGINE = 'discord',
PARAMETERS = {
  "token": "{YOUR_TOKEN_HERE}"
};
~~~~

Use the established connection to send messages to your Discord channel:

~~~~sql
INSERT INTO discord_datasource.messages (channel_id, text)
VALUES (842979385837092867, 'Hello World!');
~~~~

Query messages with a SELECT statement, but remember to always include a channel id in the WHERE clause:

~~~~sql
SELECT * FROM discord_datasource.messages
WHERE channel_id = 842979385837092867;
~~~~

Select only the rows you want:

~~~~sql
SELECT author_username, content, timestamp FROM discord_datasource.messages
WHERE channel_id = 842979385837092867
AND timestamp > '2023-10-28 3:50:01'
LIMIT 25;
~~~~
