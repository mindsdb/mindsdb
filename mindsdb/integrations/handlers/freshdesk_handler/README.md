# Freshdesk Integration

This documentation describes the integration of MindsDB with [Freshdesk](https://www.Freshdesk.com/), which provides software-as-a-service products related to customer support, sales, and other customer communications.

The integration allows MindsDB to access data from Freshdesk and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Freshdesk to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Freshdesk from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/Freshdesk_handler) as an engine.

```sql
CREATE DATABASE freshdesk_datasource
WITH
    ENGINE = 'freshdesk',
    PARAMETERS = {
      "api_key":"your_api_key_here",
      "domain": "yourcompany.freshdesk.com"
    };
```

Required connection parameters include the following:

* `api_key`: The API key for the Freshdesk account.
* `domain`: The Freshdesk domain (e.g., yourcompany.freshdesk.com).

<Tip>
For enabling, generating and deleting API access, refer [Managing access to the Freshdesk API](https://support.Freshdesk.com/hc/en-us/articles/4408889192858-Managing-access-to-the-Freshdesk-API)
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM freshdesk_datasource.table_name
LIMIT 10;
```

Retrieve data for a specific ticket by providing the id:

```sql
SELECT *
FROM freshdesk_datasource.tickets
where id="<ticket-id>";
```


<Note>
The above examples utilize `freshdesk_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Supported Tables

The Freshdesk integration supports the following tables:

* `agents` : The table lists all the agents.
* `tickets` : The table lists all the tickets.