# Discord API Handler

This handler integrates with the [Discord API](https://discord.com/developers/docs/intro) to allow users to automate various tasks, such as writing and responding to messages, answering questions, and to post announcements within the Discord platform.

## Example: Sending a message in your Discord Server

To see how the Discord handler is used, let's walk through a simple example to create a webhook and send a message.

### Create a Webhook from your Discord Server settings
We start by creating a webhook to connect to the Discord API. Currently, there is no need for an API key:

1. Go to your Disccord Server's settings
2. Go to Integrations
3. Go to Webhooks and create a New Webhook

### Utilizing Discord Handler's functions to send messages and announcements
To send a message using Discord Webhooks and Discord Handler follow these steps:
In main.py:
1. Initialize a DiscordHandler instance

```
bot = DiscordHandler(name='Connection Name', connection_data={'webhook_url':'YOUR-WEBHOOK-URL'})
```

2. Use the message method to send a message

```
bot.message(message = 'YOUR-MESSAGE', username = 'USERNAME')
```

3. Use the announce method to announce (@everyone) a message

```
bot.announce(message = 'YOUR-MESSAGE', username = 'USERNAME')
```