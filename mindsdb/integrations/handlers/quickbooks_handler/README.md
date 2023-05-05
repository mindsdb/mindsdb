# Quickbooks Handler

Quickbooks handler for MindsDB provides interfaces to connect to Quickbooks via APIs and pull data into MindsDB.

---

## Table of Contents

- [About Quickbooks](#about-Quickbooks)
  - [Quickbooks Handler Implementation](#Quickbooks-handler-implementation)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)
---
## About Quickbooks

Quickbooks is an accounting software package developed and marketed by Intuit. Quickbooks products are geared mainly toward small and medium-sized businesses and offer on-premises accounting applications as well as cloud-based versions that accept business payments, manage and pay bills, and payroll functions.

## Quickbooks Handler Implementation

This handler was implemented using the official Quickbooks API. It provides a simple and easy-to-use interface to access the Quickbooks API.


## Implemented Features

- Fetch the following TABLES
  - vendors
  - employees
  - purchases
  - accounts
  - bills
  - bill_payments
  
## TODO

- (List any pending features or improvements here)

## Example Usage
```
CREATE DATABASE my_qboo
With 
    ENGINE = "quickbooks",
    PARAMETERS = {
     "client_id": "<YOUR_CLIENT_ID>",
     "client_secret": "<YOUR_CLIENT_SECRET>",
     "realm_id":"<YOUR_REALM_ID>",
     "refresh_token":"<YOUR_REFRESH_TOKEN>",
     "environment":'<sandbox / production>'
    };

```

After setting up the Quickbooks Handler, you can use SQL queries to fetch data from Quickbooks:

```sql
SELECT *
FROM my_qboo.vendors;
```

To fetch data from the employees table:

```sql
SELECT *
FROM my_qboo.employees;
```
