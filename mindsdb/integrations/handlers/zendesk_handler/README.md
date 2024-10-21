---
title: Zendesk
sidebarTitle: Zendesk
---

This documentation describes the integration of MindsDB with [Zendesk](https://www.zendesk.com/), which provides software-as-a-service products related to customer support, sales, and other customer communications.

The integration allows MindsDB to access data from Zendesk and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Zendesk to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Zendesk from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/zendesk_handler) as an engine.

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
For enabling, generating and deleting API access, refer [Managing access to the Zendesk API](https://support.zendesk.com/hc/en-us/articles/4408889192858-Managing-access-to-the-Zendesk-API)
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

The Zendesk integration supports the following tables:

* `list_users` : The table lists all the users.
* `get_user_by_id` : The table to get all info about a single user.
* `list_tickets` : The table lists all the tickets.
* `get_ticket_by_id` : The table to get all info about a single ticket.
* `list_triggers` : The table lists all the triggers.
* `get_trigger_by_id` : The table to get all info about a single trigger.
* `list_activities` : The table lists all the activities.
* `get_activity_by_id` : The table to get all info about a single activity.