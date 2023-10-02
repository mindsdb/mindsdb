import discord
import responses


async def send_message(name, message, user_message, is_private):
    try:
        response = responses.handle_response(name, user_message)
        await message.author.send(response) if is_private else await message.channel.send(response)

    except Exception as e:
        print(e)


def run_discord_bot():
    TOKEN = "your_token"  # enter your token

    # Initialize the Discord bot
    intents = discord.Intents.default()
    intents.typing = False
    intents.presences = False
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f'{client.user} is now running')

    @client.event
    async def on_message(message):
        if message.author == client.user:
            return
        username = str(message.author)
        user_message = str(message.content)

        await send_message(username, message, user_message, is_private=False)

    client.run(TOKEN)
