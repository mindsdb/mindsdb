# Sentry Handler

Sentry application handler for MindsDB.

V1 scope:

- `projects` table for organization-scoped project discovery
- `issues` table for project-scoped operational issue inspection
- read-only `SELECT` support

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
