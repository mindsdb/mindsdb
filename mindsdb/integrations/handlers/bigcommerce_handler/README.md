---
title: BigCommerce
sidebarTitle: BigCommerce
---

This documentation describes the integration of MindsDB with [BigCommerce](https://www.bigcommerce.com/). The integration allows MindsDB to access data from BigCommerce and enhance it with AI capabilities.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

 - Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).

## Connection

Establish a connection to BigCommerce from MindsDB by executing the following SQL command and providing its [handler name](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/bigcommerce_handler) as an engine.

```sql
CREATE DATABASE bigcommerce_datasource
WITH
    ENGINE = 'bigcommerce',
    PARAMETERS = {
        "api_base": "https://api.bigcommerce.com/stores/0fh0fh0fh0/v3/",
        "access_token": "k9iexk9iexk9iexk9iexk9iexk9iexk"
    };
```

Required connection parameters include the following:

* `api_base`: The base URL of your BigCommerce store API (e.g., `https://api.bigcommerce.com/stores/YOUR_STORE_HASH/v3/`).
* `access_token`: The API token for authenticating with your BigCommerce account.

<Tip>
To obtain the API credentials for your BigCommerce store, follow the steps given below:
1. Log in to your BigCommerce store and go to the dashboard.
2. Navigate to `Settings` ⚙️ > `API` > `Store-level API accounts`.
3. Click `Create API Account` and fill in the following details:
   - **Token type**: Select `V2/V3 API token`
   - **Name**: Choose any descriptive name for the account
   - **OAuth Scopes**: Set permissions to at least `Read-only` for the following resources:
     - Orders
     - Products
     - Customers
     - Marketing
     - Order Fulfillment
4. Before clicking `Save`, copy and save the `API Path` (this is your `api_base` URL).
5. Click `Save` to create the API account.
6. Copy and securely save the `Access Token` that is displayed (you won't be able to see it again).
</Tip>

## Usage

Retrieve data from a specified table by providing the integration and table names:

```sql
SELECT *
FROM bigcommerce_datasource.orders
LIMIT 10;
```

The BigCommerce integration supports various tables including:

```sql
-- Available tables
SELECT * FROM bigcommerce_datasource.orders LIMIT 10;
SELECT * FROM bigcommerce_datasource.products LIMIT 10;
SELECT * FROM bigcommerce_datasource.customers LIMIT 10;
SELECT * FROM bigcommerce_datasource.categories LIMIT 10;
SELECT * FROM bigcommerce_datasource.pickups LIMIT 10;
SELECT * FROM bigcommerce_datasource.promotions LIMIT 10;
SELECT * FROM bigcommerce_datasource.wishlists LIMIT 10;
SELECT * FROM bigcommerce_datasource.segments LIMIT 10;
SELECT * FROM bigcommerce_datasource.brands LIMIT 10;
```

Query with filters and sorting:

```sql
-- Filter customers by name
SELECT * FROM bigcommerce_datasource.customers 
WHERE name LIKE 'George' 
ORDER BY last_name DESC;

-- Filter products by price and weight
SELECT * FROM bigcommerce_datasource.products 
WHERE price = 109 AND weight = 1;

-- Search categories by name
SELECT * FROM bigcommerce_datasource.categories 
WHERE name LIKE 'garden';
```
