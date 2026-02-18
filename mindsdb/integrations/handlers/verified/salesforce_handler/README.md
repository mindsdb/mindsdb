---
title: Salesforce
sidebarTitle: Salesforce
---

This documentation describes the integration of MindsDB with [Salesforce](https://www.salesforce.com/), the world’s most trusted customer relationship management (CRM) platform.
The integration allows MindsDB to access data from Salesforce and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Salesforce to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Salesforce from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/salesforce_handler) as an engine.

```sql
CREATE DATABASE salesforce_datasource
WITH
    ENGINE = 'salesforce',
    PARAMETERS = {
        "username": "demo@example.com",
        "password": "demo_password",
        "client_id": "3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
        "client_secret": "5A52C1A1E21DF9012IODC9ISNXXAADDA9"
    };
```

Required connection parameters include the following:

* `username`: The username for the Salesforce account.
* `password`: The password for the Salesforce account.
* `client_id`: The client ID (consumer key) from a connected app in Salesforce.
* `client_secret`: The client secret (consumer secret) from a connected app in Salesforce.

Optional connection parameters include the following:

* `is_sandbox`: The setting to indicate whether to connect to a Salesforce sandbox environment (`true`) or production environment (`false`). This parameter defaults to `false`.

<Tip>
To create a connected app in Salesforce and obtain the client ID and client secret, follow the steps given below:
1. Log in to your Salesforce account.
2. Go to `Settings` > `Open Advanced Setup` > `Apps` > `App Manager`.
3. Click `New Connected App`, select `Create a Connected App` and click `Continue`.
4. Fill in the required details, i.e., `Connected App Name`, `API Name` and `Contact Phone`.
5. Select the `Enable OAuth Settings` checkbox, set the `Callback URL` to wherever MindsDB is deployed followed by `/verify-auth` (e.g., `http://localhost:47334/verify-auth`), and choose the following OAuth scopes:
 - Manage user data via APIs (api)
 - Perform requests at any time (refresh_token, offline_access)
6. Click `Save` and then `Continue`.
7. Click on `Manage Consumer Details` under `API (Enable OAuth Settings)`, and copy the Consumer Key (client ID) and Consumer Secret (client secret).
8. Click on `Back to Manage Connected Apps` and then `Manage`.
9. Click `Edit Policies`.
10. Under `OAuth Policies`, configure the `Permitted Users` and `IP Relaxation` settings according to your security policies. For example, to enable all users to access the app without enforcing any IP restrictions, select `All users may self-authorize` and `Relax IP restrictions` respectively. Leave the `Refresh Token Policy` set to `Refresh token is valid until revoked`.
11. Click `Save`.
12. Go to `Identity` > `OAuth and OpenID Connect Settings`.
13. Ensure that the `Allow OAuth Username-Password Flows` checkbox is checked.
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM salesforce_datasource.table_name
LIMIT 10;
```

Run [SOQL](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm) queries directly on the connected Salesforce account:

```sql
SELECT * FROM salesforce_datasource (

    --Native Query Goes Here
    SELECT Name, Account.Name, Account.Industry
    FROM Contact
    WHERE Account.Industry = 'Technology'
    LIMIT 5

);
```

<Note>
The above examples utilize `salesforce_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>
