---
title: Oracle NetSuite
sidebarTitle: NetSuite
---

This documentation describes the integration of MindsDB with Oracle NetSuite using the REST Query (SuiteQL) API.
It lets you query NetSuite data in SQL and run SuiteQL directly when you need full control over filtering and joins.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. Enable Token-Based Authentication (TBA) and REST Web Services in NetSuite:
   - Setup > Company > Enable Features > SuiteCloud tab
   - Check "Token-Based Authentication" and "REST Web Services"

## Connection

Establish a connection to NetSuite from MindsDB by executing the following SQL command and providing its handler name as an engine.

```sql
CREATE DATABASE netsuite_datasource
WITH
    ENGINE = 'netsuite',
    PARAMETERS = {
        "account_id": "123456_SB1",
        "consumer_key": "ck_...",
        "consumer_secret": "cs_...",
        "token_id": "token_...",
        "token_secret": "token_secret_...",
        "rest_domain": "https://123456-sb1.suitetalk.api.netsuite.com",
        "record_types": "customer,transaction,inventoryitem"
    };
```

Required connection parameters include the following:

- `account_id`: NetSuite account/realm ID (e.g. `123456_SB1`)
- `consumer_key`: Integration consumer key
- `consumer_secret`: Integration consumer secret
- `token_id`: Access token ID
- `token_secret`: Access token secret

Optional connection parameters include the following:

- `rest_domain`: Override REST domain (defaults to `https://<account_id>.suitetalk.api.netsuite.com`, with underscores converted to dashes)
- `record_types`: Record types to expose as tables, either a comma-separated string (`"customer,transaction"`) or a JSON array (`["customer", "transaction"]`)

If `record_types` is not provided, the handler registers only a small set of tables that are commonly accessible:
`contact`, `customer`, `item`, `message`, `subsidiary`, `task`, `transaction`.

## Token-Based Authentication setup

To create the required credentials in NetSuite:

1. Create an Integration record: Setup > Integrations > Manage Integrations > New. Enable Token-Based Authentication.
2. Create/choose a role for the integration and grant:
   - Setup > REST Web Services (Full)
   - Setup > User Access Tokens (Full)
   - Record-level permissions you will query (e.g., Transactions > Sales Order, Lists > Customers).
3. Assign that role to the user.
4. Generate an Access Token: Setup > Users/Roles > Access Tokens > New.
5. Copy the Consumer Key/Secret and Token ID/Secret.

## Usage

Retrieve data from a record table (SuiteQL base table names, lowercased):

```sql
SELECT *
FROM netsuite_datasource.salesorder
WHERE id = 48;
```

Record tables:
- Use `WHERE id = ...` (or `internalId`) to fetch a record directly.
- Equality filters are pushed down to SuiteQL; other filters are applied locally.

Run SuiteQL directly using the native query syntax (recommended for complex filters):

```sql
SELECT * FROM netsuite_datasource (
    SELECT id, tranid, total
    FROM transaction
    WHERE type = 'SalesOrd'
    FETCH NEXT 5 ROWS ONLY
);
```

Limit the registered tables to what your role can access:

```sql
CREATE DATABASE netsuite_limited
WITH
    ENGINE = 'netsuite',
    PARAMETERS = {
        "account_id": "123456_SB1",
        "consumer_key": "ck_...",
        "consumer_secret": "cs_...",
        "token_id": "token_...",
        "token_secret": "token_secret_...",
        "record_types": ["customer", "salesorder", "invoice"]
    };
```

<Note>
Use the `rest_domain` parameter if your account uses a REST domain that differs from the default derived from `account_id`.
</Note>

<Note>
Access to record tables and SuiteQL depends on the NetSuite role tied to your access token.
If a query fails with 403/permission errors, ensure the role includes REST Web Services, User Access Tokens, and record-specific permissions for the tables you are querying (plus SuiteAnalytics permissions for SuiteQL).
</Note>

<Note>
The NetSuite handler is read-only. `INSERT`, `UPDATE`, and `DELETE` are not supported.
</Note>
