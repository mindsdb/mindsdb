---
title: Jira
sidebarTitle: Jira
---

This documentation describes the integration of MindsDB with [Jira](https://www.atlassian.com/software/jira/guides/getting-started/introduction), the #1 agile project management tool used by teams to plan, track, release and support world-class software with confidence.
The integration allows MindsDB to access data from Jira and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Jira to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to Jira from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/jira_handler) as an engine.

```sql
CREATE DATABASE jira_datasource
WITH
    ENGINE = 'jira',
    PARAMETERS = {
        "url": "https://example.atlassian.net",
        "username": "john_doe",
        "access_token": "abc123xyz456"
    };
```

Required connection parameters include the following:
* `url`: The base URL of the Jira instance (either Jira Cloud or Jira Server).

Optional connection parameters include the following:
* `username`: The username of the Jira account. This is required for Jira Cloud or when authentication is enabled for Jira Server.
* `access_token`: The access token of the Jira account. This is required for Jira Cloud.
* `personal_access_token`: The personal access token of the Jira account. This is required for Jira Server.

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM jira_datasource.table_name
LIMIT 10;
```

Run [JQL](https://www.atlassian.com/blog/jira/jql-the-most-flexible-way-to-search-jira-14) queries directly on the connected Jira account:

```sql
SELECT * FROM jira_datasource (

    --Native Query Goes Here
    project = 'MindsDB' AND status = 'Done'

);
```

<Note>
The above examples utilize `jira_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

## Supported Tables

The Salesforce integration supports the following tables:

* `projects`: The table containing project details.
* `issues`: The table containing issue details.