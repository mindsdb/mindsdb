
# FirestoreDB Handler

FirestoreDB handler for MindsDB provides interfaces to connect to Firestore via APIs and pull store data into MindsDB.

---

## Table of Contents

- [Firestore Handler](#firestoredb-handler)
  - [Table of Contents](#table-of-contents)
  - [About Firestore](#about-firestore)
  - [Firestore Handler Implementation](#firestore-handler-implementation)
  - [Firestore Handler Initialization](#firestore-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Firestore

Cloud Firestore is a flexible, scalable database for mobile, web, and server development from Firebase and Google Cloud
<br>
https://firebase.google.com/docs/firestore

## Firestore Handler Implementation

This handler was implemented using [google-cloud-firestore](https://github.com/googleapis/python-firestore), the Python library for the Firestore API.

## Firestore Handler Initialization

The Firestore handler is initialized with the following parameters:

- `project`: Your gcp project id
- `credentials`: Service account credentials file path with access to firestore DB

## Implemented Features

- [x] Firestore collection for any given database
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
  - [x] Support UPDATE
  - [] Support DELETE

## TODO

- [ ] Support DELETE, GROUP_BY.
- [ ] Many more

## Example Usage

The first step is to create a database with the new `firestore` engine by passing in the required `project` and `credentials` parameter:

~~~~sql
CREATE DATABASE firestore_datastore
WITH ENGINE = 'firestore',
PARAMETERS = {
    "project": "my_project",
    "credentials": "my_credentials.json"
};
~~~~

Use the established connection to query your database:

### Querying the collection data
~~~~sql
SELECT * FROM firestore_datastore.{collection_name}
~~~~

Run more advanced queries:
~~~~sql
SELECT id, name, active
FROM firestore_datastore.{collection_name}
WHERE active = true
ORDER BY name
LIMIT 5
~~~~


~~~~sql
UPDATE firestore_datastore.{collection_name}
SET name = 'name_updated'
WHERE name = 'name'
~~~~


### Components

**FirestoreDBRender**

Converts AST-query to Firestore query.

"Select" and "Update" is supported at the moment.

This method can provide different capabilities to query from Firestore:
- simple select query
- conditions
- sorting
- limit

**FirestoreDBHandler**
# TODO: Complete this



Limitations of FirestoreDBHandler
- get_columns method gets columns from first record of collection.
Because collections are schemaless, it is not possible to get all columns from collection.  

