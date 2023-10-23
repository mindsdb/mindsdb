# Instatus Handler

Instatus handler for MindsDB provides interfaces to connect with Instatus via APIs and pull the status pages.

## Instatus

Instatus is a cloud-based status page software that allows users to communicate their status using incidents and maintenances. It's a SaaS platform that helps companies create status pages for their services.

## Instatus Handler Initialization

The Instatus handler is initialized with the following parameters:

- `api_key`: Instatus API key to use for authentication

Please follow this [link](https://dashboard.instatus.com/developer) to get the api key for accessing Instatus API.

## Implemented Features

- [x] Instatus status pages table
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support column selection

## Example Usage

The first step is to create a database with the new `Instatus` engine.

```sql
CREATE DATABASE mindsdb_instatus
WITH ENGINE = 'instatus',
PARAMETERS = {
  "api_key": "<your-instatus-api-key>"
};
```

### Get your status pages

```sql
SELECT * FROM mindsdb_instatus.status_pages WHERE page_id = '<status-page-id>';
```

### Create a status page

```sql
INSERT INTO mindsdb_instatus.status_pages (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...);
```

### Update a status page

```sql
UPDATE mindsdb_instatus.status_pages
SET column1 = value1, column2 = value2, ...
WHERE page_id = '<status-page-id>';
```

### Delete a status page

```sql
DELETE FROM mindsdb_instatus.status_pages WHERE page_id = '<status-page-id>';
```