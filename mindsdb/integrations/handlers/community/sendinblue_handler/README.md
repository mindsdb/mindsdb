# Sendinblue Handler

Sendinblue handler for MindsDB provides interfaces to connect to Sendinblue via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [Sendinblue Handler](#github-handler)
  - [Table of Contents](#table-of-contents)
  - [About Sendinblue](#about-githhub)
  - [Sendinblue Handler Implementation](#sendinblue-handler-implementation)
  - [Sendinblue Handler Initialization](#sendinblue-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Sendinblue

Sendinblue is the only all-in-one digital marketing platform empowering B2B and B2C businesses, ecommerce sellers and agencies to build customer relationships through end to end digital marketing campaigns, transactional messaging, and marketing automation.
<br>
https://www.sendinblue.com/about/

## Sendinblue Handler Implementation

This handler was implemented using [sib-api-v3-sdk](https://github.com/sendinblue/APIv3-python-library), the Python SDK for Sendinblue.

## Sendinblue Handler Initialization

The Sendinblue handler is initialized with the following parameters:

- `api_key`: a required Sendinblue API key to use for authentication

Read about creating a Sendinblue API key [here](https://developers.sendinblue.com/docs).

## Implemented Features

- [x] Sendinblue Email Campaigns Table for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## TODO

- [ ] Support INSERT, UPDATE and DELETE for Email Campaigns table
- [ ] Sendinblue Contacts table
- [ ] Sendinblue Companies table
- [ ] Sendinblue Conversations table
- [ ] Sendinblue Deals table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `sendinblue` engine by passing in the required `api_key` parameter:

~~~~sql
CREATE DATABASE sib_datasource
WITH ENGINE = 'sendinblue',
PARAMETERS = {
  "api_key": "xkeysib-..."
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM sib_datasource.email_campaigns
~~~~

Run more advanced queries:

~~~~sql
SELECT  id, name
FROM sib_datasource.email_campaigns
WHERE status = 'sent'
ORDER BY name
LIMIT 5
~~~~
