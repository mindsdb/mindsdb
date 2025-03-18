---
title: Jira
sidebarTitle: Jira
---

This documentation describes the integration of MindsDB with [Jira](https://www.atlassian.com/software/jira/guides/getting-started/introduction), the #1 agile project management tool used by teams to plan, track, release and support world-class software with confidence.
The integration allows MindsDB to access data from Jira and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Salesforce to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Jira from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/jira_handler) as an engine.

```sql
CREATE DATABASE jira_datasource
WITH
    ENGINE = 'jira',
    PARAMETERS = {
        "api_base": "https://example.atlassian.net",
        "username": "john.doe@example.com",
        "password": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6"
    };
```

Required connection parameters include the following:

* `api_base`: The base URL for your Jira instance/server.
* `username`: The email address associated with your Jira account.
* `password`: The API token generated for your Jira account.

<Tip>
Refer this [guide](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/) for instructions on how to create API tokens for your account.
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM jira_datasource.table_name
LIMIT 10;
```

<Note>
The above example utilize `jira_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>