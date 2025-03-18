---
title: Microsoft Teams
sidebarTitle: Microsoft Teams
---

This documentation describes the integration of MindsDB with [Microsoft Teams](https://www.microsoft.com/en-us/microsoft-teams/group-chat-software), the ultimate messaging app for your organization.
The integration allows MindsDB to create chatbots enhanced with AI capabilities that can respond to messages in Microsoft Teams.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Microsoft Teams to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

The Microsoft Teams integration can either be used as the engine for a chatbot or as a data source. This is controlled by the `mode` parameter, which is set to `chat` by default.

### As a Chatbot Engine

Establish a connection to Microsoft Teams from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/ms_teams_handler) as an engine.

```sql
CREATE DATABASE teams_conn
WITH ENGINE = 'teams', 
PARAMETERS = {
  "client_id": "12345678-90ab-cdef-1234-567890abcdef",
  "client_secret": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
};
```

Required connection parameters include the following:

* `client_id`: The client ID of the registered Microsoft Entra ID application.
* `client_secret`: The client secret of the registered Microsoft Entra ID application.

Optional connection parameters include the following:
* `mode`: The mode indicating whether the connection is used as a chatbot engine (`chat`) or a data source (`data`). This is set to `chat` by default and therefore, it can be omitted when using the integration as a chatbot.

#### How to set up Microsoft Entra ID app registration and MindsDB chatbot

Follow the instructions given below to set up the Microsoft Entra ID app registration that will act as the chatbot:

<Steps>
  <Step title="Register a bot in the Microsoft Bot Framework portal">
    - Follow [this link](https://dev.botframework.com/bots/new) to the Microsoft Bot Framework portal and sign in with your Microsoft account.
    - Fill out the *Display name*, *Bot handle*, and, optionally, the *Long description*, but leave the *Messaging endpoint* field empty for now.
    - Set the *App type* to be `Multi Tenant` and click on *Create Microsoft App ID and password*. This will open a new tab with the Azure portal.
  </Step>
  <Step title="Register an application in the Azure portal">
    - Navigate to Microsoft Entra ID in the Azure portal, click on *Add* and then on *App registration*.
    - Click on *New registration* and fill out the *Name* and select the `Accounts in any organizational directory (Any Azure AD directory - Multitenant)` option under *Supported account types*, and click on *Register*. **Save the *Application (client) ID* for later use.**
    - Click on *Certificates & secrets* under *Manage*.
    - Click on *New client secret* and fill out the *Description* and select an appropriate *Expires* period, and click on *Add*.
    - Copy and **save the client secret in a secure location.**
    <Tip>
    If you already have an existing app registration, you can use it instead of creating a new one and skip the above steps.
    </Tip>
  </Step>
  <Step title="Configure a chatbot in the MindsDB Editor">
    - Open the MindsDB editor and create a connection to Microsoft Teams using the client ID and client secret obtained in the previous steps. Use the `CREATE DATABASE` statement as shown above.
    - Using this connection, create a chatbot with the [`CREATE CHATBOT`](/agents/chatbot) syntax, as shown in the Usage section below.
    - Run the `SHOW CHATBOTS` command and record the `webhook_token` of the chatbot you created.
  </Step>
  <Step title="Complete the bot setup in the Microsoft Bot Framework portal">
    - Navigate back to the Microsoft Bot Framework portal and fill out the messaging endpoint in the following format: `<mindsdb_url>/api/webhooks/chatbots/<webhook_token>`.
  <Tip>
  The `<mindsdb_url>` placeholder should be replaced with the URL where MindsDB instance is running. *Note that if you are running MindsDB locally, it will need to be exposed to the internet using a service like [ngrok](https://ngrok.com/).*
  </Tip>
    - Fill out the *Microsoft App ID* using the client ID obtained in the previous steps, agree to the terms, and click on *Register*.
    - Under *Add a featured channel*, click on *Microsoft Teams*, select your Microsoft Teams solution, click on *Save* and agree to the terms when prompted.
  </Step>
  <Step title="Create an application via the Developer Portal in Microsoft Teams">
    - Navigate to Microsoft Teams and then to the *Apps* tab.
    - Search for the *Developer Portal* app and add it to your workspace.
    - Open the *Developer Portal*, click on *Apps* and then on *New app*.
    - Fill out the *Name* for the app and click on *Add*.
    - Navigate to *Basic information* and fill out the required fields: *Short description*, *Long description*, *Developer or company name*, *Website*, *Privacy policy*, *Terms of use*, and *Application (client) ID*. You may also provide any additional information if you prefer.
  <Tip>
  Please note that the above fields are required for the bot to be usable in Microsoft Teams. The URLs provided in the *Website*, *Privacy policy*, and *Terms of use* fields must be valid URLs.
  </Tip>
    - Navigate to *App features* and select *Bot*.
    - Choose the *Select an existing bot* option and select the bot you created earlier in the Microsoft Bot Framework portal.
    - Select all of the scopes where bot is required to be used and click on *Save*.
    - Finally, on the navigation pane, click on *Publish* and then on *Publish to your org*.
  </Step>
</Steps>

<Info>
For the bot to be made available to the users in your organization, you will need to ask your IT administrator to approve the submission at this [link](https://admin.teams.microsoft.com/policies/manage-apps). 

Once it is approved, users can find it under the *Built for your org* section in the *Apps* tab. They can either chat with the bot directly or add it to a channel. To chat with the bot in a channel, type `@<bot_name>` followed by your message.

While waiting for approval, you can test the chatbot by clicking on *Preview in Teams* in the Developer Portal.
</Info>

### As a Data Source

Establish a connection to Microsoft Teams from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/ms_teams_handler) as an engine.

```sql
CREATE DATABASE teams_datasource
WITH ENGINE = 'teams', 
PARAMETERS = {
  "client_id": "12345678-90ab-cdef-1234-567890abcdef",
  "client_secret": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "tenant_id": "abcdef12-3456-7890-abcd-ef1234567890",
  "mode": "data"
};
```

Required connection parameters include the following:

* `client_id`: The client ID of the registered Microsoft Entra ID application.
* `client_secret`: The client secret of the registered Microsoft Entra ID application.
* `tenant_id`: The tenant ID of the Microsoft Entra ID directory.
* `mode`: The mode indicating whether the connection is used as a chatbot engine (`chat`) or a data source (`data`). This is set to `chat` by default and therefore, it becomes required when using the integration as a data source.

#### How to set up Microsoft Entra ID app registration

Follow the instructions given below to set up the Microsoft Entra ID app registration that will act as the chatbot:

<Steps>
  <Step title="Register an application in the Azure portal">
    - Navigate to Microsoft Entra ID in the Azure portal, click on *Add* and then on *App registration*.
    - Click on *New registration* and fill out the *Name* and select the `Accounts in any organizational directory (Any Azure AD directory - Multitenant)` option under *Supported account types*.
    - Select `Web` as the platform and enter URL where MindsDB has been deployed followed by /verify-auth under *Redirect URI*. For example, if you are running MindsDB locally (on https://localhost:47334), enter https://localhost:47334/verify-auth in the Redirect URIs field.
    - Click on *Register*. **Save the *Application (client) ID* and *Directory (tenant) ID* for later use.**
    - Click on *API Permissions* and then click on *Add a permission*.
    - Select *Microsoft Graph* and then click on *Delegated permissions*.
    - Search for the following permissions and select them:
      - Team.ReadBasic.All
      - Channel.ReadBasic.All
      - ChannelMessage.Read.All
      - Chat.Read
    - Click on **Add permissions**.
    - Request an administrator to grant consent for the above permissions. If you are the administrator, click on **Grant admin consent for [your organization]** and then click on **Yes**.
    - Click on *Certificates & secrets* under *Manage*.
    - Click on *New client secret* and fill out the *Description* and select an appropriate *Expires* period, and click on *Add*.
    - Copy and **save the client secret in a secure location.**
    <Tip>
    If you already have an existing app registration, you can use it instead of creating a new one and skip the above steps.
    </Tip>
  </Step>
  <Step title="Configure a MS Teams data souce in the MindsDB Editor">
    - Open the MindsDB editor and create a connection to Microsoft Teams using the client ID, client secret and tenant ID obtained in the previous steps. Use the `CREATE DATABASE` statement as shown above.
  </Step>
<Steps>

<Note>
Microsoft Entra ID was previously known as Azure Active Directory (Azure AD).
</Note>

## Usage

### As a Chatbot Engine

```sql
CREATE CHATBOT teams_chatbot
USING
    database = 'teams_conn',
    agent = 'ai_agent',
    is_running = true;
```

Learn more about [agents and chatbots here](/mindsdb_sql/agents/chatbot). Follow [this tutorial](/use-cases/ai_agents/build_ai_agents) to build your own chatbot.

### As a Data Source

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM teams_datasource.table_name
LIMIT 10;
```

#### Supported Tables

* `channels`: The table containing information about the channels in Microsoft Teams.
* `channel_messages`: The table containing information about messages from channels in Microsoft Teams.
* `chats`: The table containing information about the chats in Microsoft Teams.
* `chat_messages`: The table containing information about messages from chats in Microsoft Teams.

## Troubleshooting Guide

<Warning>
`No response from the bot`:

* **Symptoms**: The bot does not respond to messages.
* **Checklist**:
    1. Ensure the bot is correctly set up in the Microsoft Bot Framework portal.
    2. Ensure that the client ID and client secret used to create the connection are correct and valid.
    3. Ensure that the messaging endpoint is correctly set in the Microsoft Bot Framework portal with the correct webhook token.
    4. Confirm that the bot is added to the Microsoft Teams workspace.
</Warning>

<Warning>
`The bot is not available to other users`:

* **Symptoms**: The bot is not available to other users in the organization.
* **Checklist**:
    1. Ensure that the bot is published to the organization in the Microsoft Bot Framework portal.
    2. Ensure that the bot is approved by the IT administrator.

</Warning>
