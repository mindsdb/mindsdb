# Shopify Handler

Shopify handler for MindsDB provides interfaces to connect to Shopify via APIs and pull store data into MindsDB.

---

## Table of Contents

- [Shopify Handler](#shopify-handler)
  - [Table of Contents](#table-of-contents)
  - [About Shopify](#about-shopify)
  - [Shopify Handler Implementation](#shopify-handler-implementation)
  - [Shopify Handler Initialization](#shopify-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Shopify

Shopify is a complete commerce platform that lets you start, grow, and manage a business.
<br>
https://www.shopify.com/blog/what-is-shopify

## Shopify Handler Implementation

This handler was implemented using [shopify_python_api](https://github.com/Shopify/shopify_python_api), the Python SDK for Shopify.

## Shopify Handler Initialization

The Shopify handler is initialized with the following parameters:

- `shop_url`: a required URL to your Shopify store.
- `access_token`: a required access token to use for authentication.

These are the optional parameters:

- `yotpo_app_key`: token needed to access customer reviews via the Yotpo Product Reviews app.
- `yotpo_access_token`: token needed to access customer reviews via the Yotpo Product Reviews app.

If you want to query customer reviews, use the [Yotpo Product Reviews](https://apps.shopify.com/yotpo-social-reviews) app available in Shopify. To generate `yotpo_app_key` and `yotpo_access_token` keys, refer to [this](https://support.yotpo.com/docs/finding-your-yotpo-app-key-and-secret-key) and [this](https://apidocs.yotpo.com/reference/yotpo-authentication).

Watch this video on creating a Shopify access token [here](https://www.youtube.com/watch?v=4f_aiC5oTNc&t=302s).

## Implemented Features

- [x] Shopify Products Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
- [x] Shopify Customers Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
- [x] Shopify Orders Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Customer Reviews Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Inventory Level Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Carrier Service Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Shipping Zone Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Shopify Sales Channel for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Support UPDATE and DELETE for Customers table
- [ ] Support INSERT, UPDATE and DELETE for Product and Orders tables
- [ ] Shopify Payments table
- [ ] Shopify Inventory table
- [ ] Shopify Discounts table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `shopify` engine by passing in the required `shop_url` and `access_token` parameters. Optionally, you can provide additional keys, `yotpo_app_key` and `yotpo_access_token`, to access customer reviews.

~~~~sql
CREATE DATABASE shopify_datasource
WITH ENGINE = 'shopify',
PARAMETERS = {
  "shop_url": "your-shop-name.myshopify.com",
  "access_token": "shppa_...",
  "yotpo_app_key": "...",
  "yotpo_access_token": "..."
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM shopify_datasource.products
~~~~

~~~~sql
SELECT * FROM shopify_datasource.carrier_service;
~~~~

~~~~sql
SELECT * FROM shopify_datasource.shipping_zone;
~~~~

Run more advanced SELECT queries:

~~~~sql
SELECT  id, title
FROM shopify_datasource.products
WHERE status = 'active'
ORDER BY id
LIMIT 5
~~~~

It is also possible to INSERT data into your Shopify store. At the moment, only the `customers` table supports INSERT:

~~~~sql
INSERT INTO shopify_datasource.customers(first_name, last_name, email)
VALUES 
('John', 'Doe', 'john.doe@example.com')
~~~~

A limited number of columns are supported for INSERT: 'first_name', 'last_name', 'email', 'phone', 'tags' and 'currency'. Of these either 'first_name', 'last_name', 'email' or 'phone' must be provided. 

Inventory details for the products can be queried as follows:

~~~~sql
SELECT  *
FROM shopify_datasource.inventory_level
WHERE inventory_item_ids="id1,id2" AND location_ids="id1,id2"
ORDER BY available
LIMIT 5
~~~~

`inventory_item_ids` or `location_ids` have to be specified in the `where` clause of the query. 

For querying locations, you can use the `locations` table:

~~~~sql
SELECT  id, name, address
FROM shopify_datasource.locations
ORDER BY id
LIMIT 5
~~~~

For `customer_reviews` table, only SELECT is supported.

~~~~sql
SELECT  *
FROM shopify_datasource.customer_reviews
WHERE score=5
ORDER BY id
LIMIT 5
~~~~
