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

<Note>
Microsoft Entra ID was previously known as Azure Active Directory (Azure AD).
</Note>

## Usage

This integration can only be used to create chatbots for Microsoft Teams via the [`CREATE CHATBOT`](/agents/chatbot) syntax. Currently, *it cannot be used as a data source for other workloads*.

Follow the instructions given below to set up the Microsoft Teams app that will act as the chatbot:

  1. Follow [this link](https://dev.botframework.com/bots/new) to the Microsoft Bot Framework portal and sign in with your Microsoft account.
  2. Fill out the *Display name*, *Bot handle*, and, optionally, the *Long description*, but leave the *Messaging endpoint* field empty for now.
  3. Set the *App type* to be `Multi Tenant` and click on *Create Microsoft App ID and password*. This will open a new tab with the Azure portal.
  4. Click on *New registration* and fill out the *Name* and select the `Accounts in any organizational directory (Any Azure AD directory - Multitenant)` option under *Supported account types*, and click on *Register*. Record the *Application (client) ID* for later use.
  5. Click on *Certificates & secrets* under *Manage*.
  6. Click on *New client secret* and fill out the *Description* and select an appropriate *Expires* period, and click on *Add*.
  7. Copy the client secret and save it in a secure location.
  <Tip>
  If you already have an existing app registration, you can use it instead of creating a new one and skip steps 4-6.
  </Tip>

  8. Open the MindsDB Editor and create a connection to Microsoft Teams using the client ID and client secret obtained in the previous steps using the `CREATE DATABASE` command provided above
  9. Using this connection, create a chatbot using the [`CREATE CHATBOT`](/agents/chatbot) syntax:
  ```sql
  CREATE CHATBOT ms_teams_chatbot
  USING
      database = 'teams_conn',   -- this must be created with CREATE DATABASE
      agent = 'agent_name';   -- this must be created with CREATE AGENT
  ```

  10. Run the `SHOW CHATBOTS` command and record the `webhook_token` of the chatbot you created.
  11. Navigate back to the Microsoft Bot Framework portal and fill out the messaging endpoint in the following format: `<mindsdb_url>/api/webhooks/chatbots/<webhook_token>`.
  <Tip>
  The `<mindsdb_url>` is the URL of where MindsDB is running, which should be replaced with real values
  Please note that if you are running MindsDB locally, it will need to be exposed to the internet using a service like [ngrok](https://ngrok.com/).
  </Tip>

  12. Fill out the *Microsoft App ID* using the client ID obtained in the previous steps, agree to the terms, and click on *Register*.
  13. Under *Add a featured channel*, click on *Microsoft Teams*, select your Microsoft Teams solution, click on *Save* and agree to the terms when prompted.
  14. Navigate to Microsoft Teams and then to the *Apps* tab.
  15. Search for the *Developer Portal* app and add it to your workspace.
  16. Open the *Developer Portal*, click on *Apps* and then on *New app*.
  17. Fill out the *Name* for the app and click on *Add*.
  18. Navigate to *Basic information* and fill out the required fields: *Short description*, *Long description*, *Developer or company name*, *Website*, *Privacy policy*, *Terms of use*, and *Application (client) ID*. You may also provide any additional information you wish.
  <Tip>
  Please note that the above fields are required to be filled out for the bot to be usable in Microsoft Teams. The URLs provided in the *Website*, *Privacy policy*, and *Terms of use* fields must be valid URLs.
  </Tip>

  19. Navigate to *App features* and select *Bot*.
  20. Choose the *Select an existing bot* option and select the bot you created earlier in the Microsoft Bot Framework portal.
  21. Select all of the scopes where bot is required to be used and click on *Save*.
  22. Finally, on the navigation pane, click on *Publish* and then on *Publish to your org*.

For the bot to be made available to the users in your organization, you will need to ask your IT administrator to approve the submission at this [link](https://admin.teams.microsoft.com/policies/manage-apps). 

Once it is approved, users can find it under the *Built for your org* section in the *Apps* tab. They can either chat with the bot directly or add it to a channel. To chat with the bot in a channel, type `@<bot_name>` followed by your message.

While waiting for approval, you can test the chatbot by clicking on *Preview in Teams* in the Developer Portal.

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