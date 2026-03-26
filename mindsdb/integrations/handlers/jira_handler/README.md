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
        "jira_url": "https://example.atlassian.net",
        "jira_username": "john.doe@example.com",
        "jira_api_token": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6",
        "cloud": true
    };
```

Required connection parameters include the following:

* `jira_url`: The base URL for your Jira instance/server.
* `jira_username`: The email address associated with your Jira account.
* `jira_api_token`: The API token generated for your Jira Cloud account.
* `cloud`: (Optional) Set to `true` for Jira Cloud or `false` for Jira Server. Defaults to `true`.

For Jira Server connections, set `cloud` to `false` and use either:

* `jira_personal_access_token`: A Jira Server personal access token, or
* `jira_password`: With `jira_username`, for basic authentication.

Example for Jira Server using a personal access token:

```sql
CREATE DATABASE jira_server
WITH
    ENGINE = 'jira',
    PARAMETERS = {
        "jira_url": "https://jira.my-company.internal",
        "jira_username": "john.doe@example.com",
        "jira_personal_access_token": "server-personal-access-token",
        "cloud": false
    };
```

<Tip>
Refer this [guide](https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/) for instructions on how to create API tokens for your account.
</Tip>

## Usage

The integration exposes the following tables:

* `projects`: Jira projects accessible to the connection.
* `issues`: Normalized issue data, including summary, description, status, priority, assignee, and timestamps.
* `attachments`: Attachments for the fetched issues.
* `comments`: Comments for the fetched issues.
* `users`: Jira users available to the current context.
* `groups`: Jira user groups.

Query a table by providing the integration and table names:

```sql
SELECT *
FROM jira_datasource.issues
LIMIT 10;
```

Filter by issue or project identifiers to reduce API calls:

```sql
SELECT key, summary, status, assignee
FROM jira_datasource.issues
WHERE project_key = 'ENG'
LIMIT 20;
```

Fetch related attachments or comments for a specific issue:

```sql
SELECT filename, content_url, created
FROM jira_datasource.attachments
WHERE issue_key = 'ENG-123';
```

<Note>
The above examples utilize `jira_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>
