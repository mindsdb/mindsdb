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

## Supported Tables

The Salesforce integration supports the following tables:

* `Account`: The table containing account information.
* `Contact`: The table containing contact information for people you do business with.
* `Opportunity`: The table containing sales opportunities.
* `Lead`: The table containing potential sales leads.
* `Task`: The table containing tasks and activities.
* `Event`: The table containing calendar events.
* `User`: The table containing user information.
* `Product2`: The table containing product information.
* `Pricebook2`: The table containing price book information.
* `PricebookEntry`: The table containing price book entries.
* `Order`: The table containing order information.
* `OrderItem`: The table containing order items.
* `Case`: The table containing customer service cases.
* `Campaign`: The table containing marketing campaigns.
* `CampaignMember`: The table containing campaign members.
* `Contract`: The table containing contract information.
* `Asset`: The table containing asset information.