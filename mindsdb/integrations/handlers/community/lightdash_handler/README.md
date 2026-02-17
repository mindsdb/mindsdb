# Lightdash Handler

This handler allows you to interact with a Lightdash instance

## About Lightdash

Lightdash instantly turns your dbt project into a full-stack BI platform. Analysts write metrics and Lightdash enables self-serve for the entire business.

## Lightdash Handler Initialization

You can create the database like so:

```sql
CREATE DATABASE lightdash_datasource
WITH ENGINE = "lightdash",
PARAMETERS = {
  "api_key": "...",
  "base_url": "https://..."
};
```

To select from various tables, you can use `SELECT` statement. You must provide a package for this to work.

```sql
SELECT * FROM lightdash_datasource.user;
```

```sql
SELECT firstName FROM npm_datasource.maintainers;
```

Some tables requre additional parameters that can be passed through `WHERE` clause separated by `AND`s:

```sql
SELECT * FROM lightdash_datasource.project_table
WHERE project_uuid='....';
```

## Available tables

- `user`: details of authenticated user
- `user_abilities`: list of abilities of authenticated user
- `org`: details of organization of authenticated user
- `org_projects`: list of projects under authenticated user's organization
- `org_members`: list of members of authenticated user's organization
- `project_table`: details of project defined by `project_uuid`
- `warehouse_connection`: details of the warehouse to which project with `project_uuid` is connected
- `dbt_connection`: details of dbt connection to which project with `project_uuid` is connected
- `dbt_env_vars`: list of environment variables of dbt connection to which project with `project_uuid` is connected
- `charts`: list of charts in project with `project_uuid`
- `spaces`: list of spaces in project with `project_uuid`
- `access`: list of users with access to project with `project_uuid`
- `validation`: list of validation results of the project with `project_uuid`
- `dashboards`: list of dashboards defined in space `space_uuid` of project `project_uuid`
- `queries`: list of queries in the space `space_uuid` of project `project_uuid`
- `chart_history`: history of changes of chart `chart_uuid`
- `chart_config`: configuration of chart defined by version `version_uuid` and chart `chart_uuid`
- `chart_additional_metrics`: additional metrices used in chart defined by version `version_uuid` and chart `chart_uuid`
- `chart_table_calculations`: table calculations used in chart defined by version `version_uuid` and chart `chart_uuid`
- `scheduler_logs`: logs of scheduler in project `project_uuid`
- `scheduler`: details of scheduler with `scheduler_uuid`
- `scheduler_jobs`: jobs scheduled by scheduler with `scheduler_uuid`
- `scheduler_job_status`: status of a job with `job_id`
