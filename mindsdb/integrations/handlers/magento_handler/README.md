# Magento Handler

Magento handler for MindsDB provides interfaces to connect to Magento via APIs and pull store data into MindsDB.

---

## Table of Contents

- [Magento Handler](#magento-handler)
  - [Table of Contents](#table-of-contents)
  - [About Magento](#about-magento-2)
  - [Magento Handler Implementation](#magento-handler-implementation)
  - [Magento Handler Initialization](#magento-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About Magento 2

Magento 2 is an open-source eCommerce platform, written in the general-purpose coding language PHP across multiple frameworks.
<br/>
https://business.adobe.com/products/magento/magento-commerce.html

## Magento Handler Implementation

This handler was implemented using [my_magento](https://github.com/TDKorn/my-magento), the Python SDK for magento.

## Magento Handler Initialization

The magento handler is initialized with the following parameters:

- `domain`: a required URL to your magento store.
- `username`: a required username to use for authentication.
- `password`: a required password to use for authentication.

## Implemented Features

- [x] Magento Products Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support Insert
  - [x] Support DELETE
- [x] Magento Customers Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
  - [x] Support WHERE
- [x] Magento Orders Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
  - [x] Support WHERE
- [x] Magento Categories Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
  - [x] Support WHERE
- [x] Magento invoices Table for a given Store
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support UPDATE
  - [x] Support INSERT
  - [x] Support DELETE
  - [x] Support WHERE

## Example Usage

The first step is to create a database with the new `magento` engine by passing in the required `domain`, `username` and `password` parameters. 

~~~~sql
CREATE DATABASE magento_datasource
WITH ENGINE = 'magento',
PARAMETERS = {
  "domain": "your-shop-domain",
  "username": "...",
  "password": "..."
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM magento_datasource.products
~~~~

~~~~sql
SELECT * FROM magento_datasource.orders;
~~~~

~~~~sql
SELECT * FROM magento_datasource.customers;
~~~~

Run more advanced SELECT queries:

~~~~sql
SELECT  id, title
FROM magento_datasource.products
WHERE price = '34'
ORDER BY id
LIMIT 5
~~~~

It is also possible to INSERT data into your Magento store. At the moment, only the `customers` and `products` table supports INSERT:

~~~~sql
INSERT INTO magento_datasource.customers(first_name, last_name, email)
VALUES 
('John', 'Doe', 'john.doe@example.com')
~~~~

A limited number of columns are supported for INSERT: 'first_name', 'last_name', 'email', 'phone', 'tags' and 'currency'. Of these either 'first_name', 'last_name', 'email' or 'phone' must be provided. 

It is also possible to DELETE data into your Magento store. At the moment, only the `customers` and `products` table supports DELETE:

~~~~sql
DELETE FROM magento_datasource.customers
WHERE first_name = 'John'
AND last_name = 'Doe'
AND email = 'john.doe@example.com';
~~~~

~~~~sql
DELETE FROM magento_datasource.orders
WHERE id=2;
~~~~


