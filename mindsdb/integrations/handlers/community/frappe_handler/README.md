# Frappe API Handler

This handler integrates with the [Frappe API](https://frappeframework.com/docs/v14/user/en/api/rest) to create and read Frappe Documents.


## Connect to the Frappe API
We start by creating a database to connect to the Frappe API. You'll need an [access token](https://frappeframework.com/docs/v14/user/en/api/rest) and the domain you want to send API requests to.

Example
```
CREATE DATABASE my_frappe
WITH
  ENGINE = 'frappe'
  PARAMETERS = {
    "access_token": "TOKEN_HERE",
    "domain": "DOMAIN_HERE" // e.g. https://mindsdbfrappe.com
  };
```

## Select Documents
To see if the connection was successful, try searching for all documents of a certain type. Currently, only the name is provided. You can see all of your document types at the URL `[YOUR_FRAPPE_DOMAIN]/app/doctype`

```
SELECT *
FROM my_frappe.documents
WHERE doctype = 'Expense Claim';
```

Each row should look like this:

| doctype      | data                  |
|--------------| ----------------------|
| Expense Claim| { "name": "Claim 1" } |

To get a full document, provide the name along with the type:

```
SELECT *
FROM my_frappe.documents
WHERE doctype = 'Expense Claim' AND name = 'Claim 1'
```

## Insert Documents

To create a new document, insert it as a JSON string ([see creating documents](https://frappeframework.com/docs/v14/user/en/api/rest#create))

```
INSERT INTO my_frappe.documents (doctype, data)
VALUES ('Expense Claim', '{ "posting_date": "2023-05-15", "company": "MindsDB", "amount": 100" }')
```