# Microsoft Teams Handler

Microsoft Teams handler for MindsDB provides interfaces to connect to Microsoft Teams via a webhook and send messages through MindsDB.

---

## Table of Contents

- [Microsoft Teams Handler](#microsoft-teams-handler)
  - [Table of Contents](#table-of-contents)
  - [About Microsoft Teams](#about-microsoft-teams)
  - [Microsoft Teams Handler Implementation](#microsoft-teams-handler-implementation)
  - [Microsoft Teams Handler Initialization](#microsoft-teams-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Microsoft Teams

Microsoft Teams is the ultimate messaging app for your organization—a workspace for real-time collaboration and communication, meetings, file and app sharing, and even the occasional emoji! All in one place, all in the open, all accessible to everyone.
<br>
https://support.microsoft.com/en-us/topic/what-is-microsoft-teams-3de4d369-0167-8def-b93b-0eb5286d7a29

## Microsoft Teams Handler Implementation

  This handler was implemented using [msal](https://github.com/AzureAD/microsoft-authentication-library-for-python) for authentication and [Requests](https://github.com/psf/requests) to submit requests to the Microsoft Graph API.

## Microsoft Teams Handler Initialization

The Microsoft Teams handler is initialized with the following parameters:

- `client_id`: The client ID of the registered Microsoft Entra ID application.
- `client_secret`: The client secret of the registered Microsoft Entra ID application.
- `tenant_id`: The tenant ID of the registered Microsoft Entra ID application.

Note: Microsoft Entra ID was previously known as Azure Active Directory (Azure AD).

The parameters given above can be obtained by registering an application in Microsoft Entra ID by following these steps,
1. Go to the [Azure Portal](https://portal.azure.com/#home) and sign in with your Microsoft account.
2. Locate the **Microsoft Entra ID** service and click on it.
3. Click on **App registrations** and then click on **New registration**.
4. Enter a name for your application and select the **Accounts in this organizational directory only** option for the **Supported account types** field.
5. Keep the **Redirect URI** field empty and click on **Register**.
6. Copy the **Application (client) ID** and paste it as the `client_id` parameter, and copy the **Directory (tenant) ID** and paste it as the `tenant_id` parameter.
7. Click on **Certificates & secrets** and then click on **New client secret**.
8. Enter a description for your client secret and select an expiration period.
9. Click on **Add** and copy the generated client secret and paste it as the `client_secret` parameter.
10. Click on **Authentication** and then click on **Add a platform**.
11. Select **Web** and enter the following URLs in the **Redirect URIs** field:
    - `https://cloud.mindsdb.com/verify-auth`
    - `http://localhost:47334/verify-auth` (for local development)

You can find more information about creating app registrations [here](https://docs.microsoft.com/en-us/graph/auth-register-app-v2).

When the above is statement is executed with the given parameters, the handler will open a browser window and prompt you to sign in with your Microsoft account. 

The handler will then act (via the app registration) as the signed in user and will submit requests to the Microsoft Graph API. This is done using the concept of [delegated permissions](https://docs.microsoft.com/en-us/graph/auth/auth-concepts#delegated-permissions).

## Implemented Features

- [x] MS Teams Chats Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] MS Teams ChatMessages Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
- [x] MS Teams Channels Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] MS Teams ChannelMessages Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT

## TODO

- [ ] MS Teams ChatMessageReplies Table
- [ ] MS Teams ChannelMessageReplies Table

## Example Usage

The first step is to create a database with the new `teams` engine by passing in the required parameters:

~~~~sql
CREATE DATABASE teams_datasource
WITH ENGINE = 'teams',
PARAMETERS = {
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "tenant_id": "your-tenant-id"
};
~~~~

  Use the established connection to query your chats:

~~~~sql
SELECT * FROM teams_datasource.chats
~~~~

Now, post a message to a chat:

~~~~sql
INSERT INTO teams_datasource.chat_messages (chatId, body_content)
VALUES
('your-chat-id', 'Hello from MindsDB!');
~~~~

You can also do the same for channels:

~~~~sql
SELECT * FROM teams_datasource.channels
~~~~

~~~~sql
INSERT INTO teams_datasource.channel_messages (channelIdentity_teamId, channelIdentity_channelId, body_content)
VALUES
('your-team-id', 'your-channel-id', 'Hello from MindsDB!');
~~~~

## Create a Microsoft Teams Chatbot

While the Microsoft Teams handler allows you to read/send messages to a chat or channel as shown above, it is also possible to create a chat bot that will listen to messages sent to a chat or channel and respond to them.

### Step 1: Create a Microsoft Teams Data Source

As shown above, create a database with the new `teams` engine by passing in the required parameters:

~~~~sql
CREATE DATABASE teams_datasource
WITH ENGINE = 'teams',
PARAMETERS = {
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  "tenant_id": "your-tenant-id"
};
~~~~

Note: The chat bot will assume the identity of the user who signed in with their Microsoft account when the database was created.

### Step 2: Create an [Agent](https://docs.mindsdb.com/agents/agent)

An agent is created by combining a [Model](https://docs.mindsdb.com/model-types) (conversational) with optional Skills.

Here, we will create an agent without any skills, however, it is possilbe to create agents with skills such as the ability to answer questions from a knowledge base.

#### Step 2.1: Create a Conversational Model

Create a conversational model using either the `LangChain` or `LlamaIndex` integrations. Given below is an example of a conversational model created using the `LlamaIndex` integration:

~~~~sql
CREATE ML_ENGINE llama_index_engine
FROM llama_index
USING openai_api_key='your-openai-api-key';
~~~~

~~~~sql
CREATE MODEL llama_index_convo_model
PREDICT answer
USING
  engine = 'llama_index_engine',
  mode = 'conversational',
  prompt = 'answer users questions as a helpful assistant',
  user_column = 'question',
  assistant_column = 'answer';
~~~~

#### Step 2.2: Create an Agent using the Conversational Model

Let's create an agent using the conversational model created above:

~~~~sql
CREATE AGENT convo_agent
USING
    model = 'llama_index_convo_model',
~~~~

### Step 3: Create a [Chatbot](https://docs.mindsdb.com/agents/chatbot)

Finally, create a chatbot using the agent and the Microsoft Teams data source created above:

~~~~sql
CREATE CHATBOT teams_chatbot
USING
    database = 'teams_datasource',
    agent = 'convo_agent',
    enable_dms = true
~~~~

Note: The `enable_dms` parameter is optional and is the initially supported mode of talking to a chatbot. A chatbot responds to direct messages.
