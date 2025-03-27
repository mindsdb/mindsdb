---
title: Microsoft Teams
sidebarTitle: Microsoft Teams
---

This documentation describes the integration of MindsDB with [Microsoft Teams](https://www.microsoft.com/en-us/microsoft-teams/group-chat-software), the ultimate messaging app for your organization.
The integration allows MindsDB to access data from Microsoft Teams and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To connect Microsoft Teams to MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Microsoft Teams from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/ms_teams_handler) as an engine.

```sql
CREATE DATABASE teams_datasource
WITH ENGINE = 'teams', 
PARAMETERS = {
  "client_id": "12345678-90ab-cdef-1234-567890abcdef",
  "client_secret": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
  "tenant_id": "abcdef12-3456-7890-abcd-ef1234567890"
};
```

Required connection parameters include the following:

* `client_id`: The client ID of the registered Microsoft Entra ID application.
* `client_secret`: The client secret of the registered Microsoft Entra ID application.
* `tenant_id`: The tenant ID of the Microsoft Entra ID directory.

Optional connection parameters include the following:

* `permission_mode`: The type of permissions used to access data in Microsoft Teams. Can be either `delegated` (default) or `application`. 

<Tip>
The `delegated` permission mode requires user sign-in and allows the app to access data on behalf of the signed-in user. The `application` permission mode does not require user sign-in and allows the app to access data without a user context. You can learn more about permission types in the [Microsoft Graph permissions documentation](https://learn.microsoft.com/en-us/graph/auth/auth-concepts#delegated-and-application-permissions).
Note that application permissions generally require higher privileges and admin consent compared to delegated permissions, as they allow broader access to organizational data without user context.
</Tip>

<Note>
Microsoft Entra ID was previously known as Azure Active Directory (Azure AD).
</Note>

### How to set up the Microsoft Entra ID app registration

Follow the instructions below to set up the Microsoft Teams app that will be used to connect with MindsDB.

<Steps>
  <Step title="Register an application in the Azure portal">
    - Navigate to Microsoft Entra ID in the Azure portal, click on *Add* and then on *App registration*.
    - Click on *New registration* and fill out the *Name* and select the `Accounts in any organizational directory (Any Azure AD directory - Multitenant)` option under *Supported account types*.
    - If you chose the `application` permission mode you may skip this step, but if you are using `delegated` permissions, select `Web` as the platform and enter URL where MindsDB has been deployed followed by /verify-auth under *Redirect URI*. For example, if you are running MindsDB locally (on https://localhost:47334), enter https://localhost:47334/verify-auth in the Redirect URIs field.
    - Click on *Register*. **Save the *Application (client) ID* and *Directory (tenant) ID* for later use.**
    - Click on *API Permissions* and then click on *Add a permission*.
    - Select *Microsoft Graph* and then click on either *Delegated permissions* or *Application permissions* based on the permission mode you have chosen.
    - Search for the following permissions and select them:
      - `delegated` permission mode:
        - Team.ReadBasic.All
        - Channel.ReadBasic.All
        - ChannelMessage.Read.All
        - Chat.Read
      - `application` permission mode:
        - Group.Read.All
        - ChannelMessage.Read.All
        - Chat.Read.All
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

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM  teams_datasource.table_name
LIMIT 10;
```

<Note>
The above example utilize `teams_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Supported Tables

* `teams`: The table containing information about the teams in Microsoft Teams.
* `channels`: The table containing information about the channels in Microsoft Teams.
* `channel_messages`: The table containing information about messages from channels in Microsoft Teams.
* `chats`: The table containing information about the chats in Microsoft Teams.
* `chat_messages`: The table containing information about messages from chats in Microsoft Teams.
