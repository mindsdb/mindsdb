# Sentry Handler

Sentry application handler for MindsDB.

V1 scope:

- `projects` table for organization-scoped project discovery
- `issues` table for project-scoped operational issue inspection
- `logs` table for Explore-backed log inspection
  - includes curated columns plus `extra_json` for raw additional event context
- `logs_timeseries` table for Explore-backed log volume over time
- read-only `SELECT` support

Internal organization:

- `issue/` owns the current `projects` and `issues` flow
- `explore/` owns the Explore-backed `logs` and `logs_timeseries` flow
- `sentry_client.py` and `connection_args.py` stay at the package root as shared/common pieces
- `sentry_handler.py` remains the public compatibility entrypoint

Example connection:

```sql
CREATE DATABASE sentry_datasource
WITH ENGINE = 'sentry',
PARAMETERS = {
  "auth_token": "sntrys_xxx",
  "organization_slug": "talentify",
  "project_slug": "mktplace",
  "environment": "production"
};
```
