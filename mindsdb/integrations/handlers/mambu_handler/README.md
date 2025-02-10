# HubSpot Handler

HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.

---

## Table of Contents

- [HubSpot Handler](#hubspot-handler)
  - [Table of Contents](#table-of-contents)
  - [About HubSpot](#about-hubspot)
  - [HubSpot Handler Implementation](#hubspot-handler-implementation)
  - [HubSpot Handler Initialization](#hubspot-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About HubSpot

HubSpot is a CRM platform with all the software, integrations, and resources you need to connect your marketing, sales, content management, and customer service.
<br>
https://www.hubspot.com/products?hubs_content=www.hubspot.com%2F&hubs_content-cta=All%20Products%20and%20Features

## HubSpot Handler Implementation

This handler was implemented using [hubspot-api-client
](https://github.com/HubSpot/hubspot-api-python), the Python library for the HubSpot API.

## HubSpot Handler Initialization

The HubSpot handler is initialized with the following parameters:

- `access_token`: a HubSpot access token. You can find your access token in the HubSpot Private Apps Page. [Read more](https://developers.hubspot.com/docs/api/private-apps).

## Implemented Features

- [x] HubSpot Companies Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE
- [x] HubSpot Contacts Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE
- [x] HubSpot Deals Intents Table for a given account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [x] Support DELETE

## TODO

- [ ] HubSpot Leads table
- [ ] HubSpot Products table
- [ ] Many more

## Example Usage

The first step is to create a database with the new `hubspot` engine by passing in the required `access_token` parameter:

~~~~sql
CREATE DATABASE hubspot_datasource
WITH ENGINE = 'hubspot',
PARAMETERS = {
  "access_token": "..."
};
~~~~

Use the established connection to query your database:

### Querying the Companies Data
~~~~sql
SELECT * FROM hubspot_datasource.companies
~~~~

or, for the `contacts` table
~~~~sql
SELECT * FROM hubspot_datasource.contacts
~~~~

or, for the `deals` table
~~~~sql
SELECT * FROM hubspot_datasource.deals
~~~~

Run more advanced queries:

~~~~sql
SELECT name, industry
FROM hubspot_datasource.companies
WHERE city = 'bangalore'
ORDER BY name
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.companies(name)
VALUES('company_name')
~~~~

~~~~sql
UPDATE hubspot_datasource.companies
SET name = 'company_name_updated'
WHERE name = 'company_name'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.companies
WHERE name = 'company_name_updated'
~~~~

~~~~sql
SELECT email, company
FROM hubspot_datasource.contacts
WHERE company = 'company_name'
ORDER BY email
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.contacts(email)
VALUES('contact_email')
~~~~

~~~~sql
UPDATE hubspot_datasource.contacts
SET email = 'contact_email_updated'
WHERE email = 'contact_email'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.contacts
WHERE email = 'contact_email_updated'
~~~~

~~~~sql
SELECT dealname, amount
FROM hubspot_datasource.deals
WHERE dealstage = 'deal_stage_name'
ORDER BY dealname
LIMIT 5
~~~~

~~~~sql
INSERT INTO hubspot_datasource.deals(dealname)
VALUES('deal_name')
~~~~

~~~~sql
UPDATE hubspot_datasource.deals
SET dealname = 'deal_name_updated'
WHERE dealname = 'deal_name'
~~~~

~~~~sql
DELETE FROM hubspot_datasource.deals
WHERE dealname = 'deal_name_updated'
~~~~
