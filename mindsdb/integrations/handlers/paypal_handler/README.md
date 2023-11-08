# PayPal Handler

PayPal handler for MindsDB provides interfaces to connect to PayPal via APIs and pull data into MindsDB.

---

## Table of Contents

- [PayPal Handler](#paypal-handler)
  - [Table of Contents](#table-of-contents)
  - [About PayPal](#about-paypal)
  - [PayPal Handler Implementation](#paypal-handler-implementation)
  - [PayPal Handler Initialization](#paypal-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About PayPal

PayPal is an online payment system that makes paying for things online and sending and receiving money safe and secure.
<br>
https://www.bankrate.com/finance/credit-cards/guide-to-using-paypal/

## PayPal Handler Implementation

This handler was implemented using [PayPal-Python-SDK](https://github.com/paypal/PayPal-Python-SDK), the Python SDK for PayPal RESTful APIs.

## PayPal Handler Initialization

The PayPal handler is initialized with the following parameters:

- `mode`: The mode of the PayPal API. Can be `sandbox` or `live`.
- `client_id`: The client ID of the PayPal API.
- `client_secret`: The client secret of the PayPal API.

## Implemented Features

- [x] PayPal Payments Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

- [x] PayPal Invoices Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  
- [x] PayPal Subscriptions table for a given account 
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection


## TODO

- [ ] Support INSERT, UPDATE and DELETE for the Payments table
- [ ] PayPal Orders table
- [ ] PayPal Payouts table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `paypal` engine by passing in the required parameters:

~~~~sql
CREATE DATABASE paypal_datasource
WITH ENGINE = 'paypal',
PARAMETERS = {
  "mode": "sandbox",
  "client_id": "EBWKjlELKMYqRNQ6sYvFo64FtaRLRR5BdHEESmha49TM",
  "client_secret": "EO422dn3gQLgDbuwqTjzrFgFtaRLRR5BdHEESmha49TM"
};
~~~~

Use the established connection to query your database:

Query Payments_table: 
~~~~sql
SELECT * FROM paypal_datasource.payments
~~~~

Query Invoices_table: 
~~~~sql
SELECT * FROM paypal_datasource.invoices
~~~~

Query Subscriptions_table:
~~~~sql
SELECT * FROM paypal_datasource.subscriptions
~~~~

Run more advanced queries:

`Payments_table` 
~~~~sql
SELECT  intent, cart
FROM paypal_datasource.payments
WHERE state = 'approved'
ORDER BY id
LIMIT 5
~~~~

`Invoices_table`

Query Invoices with specific columns:

~~~~sql
SELECT invoice_number, total_amount, status FROM paypal_datasource.invoices
~~~~

Query Invoices with conditions and ordering:

~~~~sql
SELECT invoice_number, total_amount
FROM paypal_datasource.invoices
WHERE status = 'PAID'
ORDER BY total_amount DESC
LIMIT 10
~~~~

`Subscriptions_table`
Query Subscriptions with specific columns:

~~~~sql
SELECT id, name FROM paypal_datasource.subscriptions
~~~~

Query Subscriptions with conditions and ordering:

~~~~sql
SELECT id , state, name 
FROM paypal_datasource.subscriptions 
WHERE state ="CREATED" 
LIMIT 5
~~~~


`Orders_table`
Query Orders with specific columns:

~~~~sql
SELECT id, name FROM paypal_datasource.orders
~~~~

Query Orders with conditions and ordering:

~~~~sql
SELECT id , state, amount 
FROM paypal_datasource.orders 
WHERE ids = ("{id1}","{id2}")
ORDER BY total_amount DESC
~~~~
