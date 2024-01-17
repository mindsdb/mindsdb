# ZipCodeBase Handler

ZipCodeBase handler for MindsDB provides interfaces to connect to ZipCodeBase via APIs and import zipcode data into MindsDB.

---

## Table of Contents

- [ZipCodeBase Handler](#zipcodebase-handler)
  - [Table of Contents](#table-of-contents)
  - [About ZipCodeBase](#about-zipcodebase)
  - [ZipCodeBase Handler Implementation](#zipcodebase-handler-implementation)
  - [ZipCodeBase Handler Initialization](#zipcodebase-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About ZipCodeBase

[Zipcodebase.com](https://zipcodebase.com/) is the perfect tool to perform postal code validation, lookups and other calculative tasks, such as postal code distance calculations. Zipcodebase offers a wide range of endpoints that give you access to any type of data you might need. 

## ZipCodeBase Handler Implementation

This handler was implemented using the `requests` library that makes http calls to https://app.zipcodebase.com/documentation.

## ZipCodeBase Handler Initialization

The ZipCodeBase handler is initialized with the following parameters:

- `api_key`: API Key used to authenticate with ZipCodeBase

Read about creating an API Key [here](https://zipcodebase.com/).

## Implemented Features

- [x] ZipCodeBase 
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## Example Usage

The first step is to create a database with the new `zipcodebase` engine. 

~~~~sql
CREATE DATABASE mindsdb_zipcodebase
WITH ENGINE = 'zipcodebase',
PARAMETERS = {
  "api_key": ""
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_zipcodebase.code_to_location where codes="10005";
~~~~

~~~~sql
SELECT * FROM mindsdb_zipcodebase.codes_within_radius WHERE code="10005" AND radius="100" AND country="us";
~~~~

~~~~sql
SELECT * FROM mindsdb_zipcodebase.codes_by_city WHERE city="Amsterdam" AND country="nl";
~~~~

~~~~sql
SELECT * FROM mindsdb_zipcodebase.codes_by_state WHERE state="Noord-Holland" AND country="nl";
~~~~

~~~~sql
SELECT * FROM mindsdb_zipcodebase.states_by_country WHERE country="de";
~~~~
