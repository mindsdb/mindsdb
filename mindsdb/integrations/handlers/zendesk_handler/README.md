---
title: Zendesk
sidebarTitle: Zendesk
---

This documentation describes the integration of MindsDB with [Zendesk](https://www.zendesk.com/), the world’s most trusted customer relationship management (CRM) platform.
The integration allows MindsDB to access data from Zendesk and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Salesforce to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Salesforce from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/zendesk_handler) as an engine.

```sql
CREATE DATABASE zendesk_datasource
WITH
    ENGINE = 'zendesk',
    PARAMETERS = {
      "api_key":"api_key",
      "sub_domain": "sub_domain",
      "email":"email"
    };
```

Required connection parameters include the following:

* `api_key`: The api key for the Zendesk account.
* `sub_domain`: The sub domain for the Zendesk account.
* `email`: The email ID of the account.

<Tip>
To create a connected app in Salesforce and obtain the client ID and client secret, follow the steps given below:
1. Log in to your Salesforce account.
2. Go to `Setup` > `Apps` > `App Manager`.
3. Click `New Connected App`.
4. Fill in the required details. Esure that the `Enable OAuth Settings` checkbox is checked, set the `Callback URL` to wherever MindsDB is deployed followed by `/verify-auth` (e.g., `http://localhost:47334/verify-auth`), and choose the appropriate OAuth scopes.
5. Click `Save`.
6. Copy the `Consumer Key` (client ID) and `Consumer Secret` (client secret) from the connected app details under `Consumer Key and Secret`.
7. Go to `Setup` > `Apps` > `Connected Apps` > `Manage Connected Apps`.
8. Click on the connected app name.
9. Click `Edit Policies`.
10. Under `OAuth Policies`, ensure that the `Permitted Users` is set to `All users may self-authorize` and `IP Relaxation` is set to `Relax IP restrictions`.
11. Click `Save`.
12. Go to `Setup` > `Identity` > `OAuth and OpenID Connect Settings`.
13. Ensure that the `Allow OAuth Username-Password Flows` checkbox is checked.
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM zendesk_datasource.table_name
LIMIT 10;
```

<Note>
The above examples utilize `zendesk_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Supported Tables

The Salesforce integration supports the following tables:

* ``: The table containing contact information for people you do business with.
* ``