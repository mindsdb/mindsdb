# Stripe Handler

Stripe handler for MindsDB provides interfaces to connect to Stripe via APIs and pull store data into MindsDB.

---

## Table of Contents

- [Stripe Handler](#stripe-handler)
  - [Table of Contents](#table-of-contents)
  - [About Stripe](#about-stripe)
  - [Stripe Handler Implementation](#stripe-handler-implementation)
  - [Stripe Handler Initialization](#stripe-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Stripe

Stripe is a payment services provider that lets merchants accept credit and debit cards or other payments.
<br>
https://www.nerdwallet.com/article/small-business/what-is-stripe

## Stripe Handler Implementation

This handler was implemented using [stripe-python](https://github.com/stripe/stripe-python), the Python library for the Stripe API.

## Stripe Handler Initialization

The Stripe handler is initialized with the following parameters:

- `api_key`: a Stripe API key. You can find your API keys in the Stripe Dashboard. [Read more](https://stripe.com/docs/keys).

## Implemented Features

- [x] Stripe Products Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Stripe Customers Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] Stripe Payment Intents Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Support INSERT, UPDATE and DELETE for Products, Customers and Payment Intents tables
- [ ] Stripe Payouts table
- [ ] Stripe Refunds table
- [ ] Stripe Charges table
- [ ] Stripe Balance table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `stripe` engine by passing in the required `api_key` parameter:

~~~~sql
CREATE DATABASE stripe_datasource
WITH ENGINE = 'stripe',
PARAMETERS = {
  "api_key": "sk_..."
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM stripe_datasource.customers
~~~~

Run more advanced queries:

~~~~sql
SELECT  name, email
FROM shopify_datasource.customers
WHERE deliquent = 'false'
ORDER BY name
LIMIT 5
~~~~
