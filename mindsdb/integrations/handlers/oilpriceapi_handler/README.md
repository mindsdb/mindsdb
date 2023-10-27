# OilPriceAPI Handler

OilPriceAPI handler for MindsDB provides interfaces to connect to OilPriceAPI via APIs and Oil Price data into MindsDB.

---

## Table of Contents

- [OilPriceAPI Handler](#oilpriceapi-handler)
  - [Table of Contents](#table-of-contents)
  - [About OilPriceAPI](#about-oilpriceapi)
  - [OilPriceAPI Handler Implementation](#oilpriceapi-handler-implementation)
  - [OilPriceAPI Handler Initialization](#oilpriceapi-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About OilPriceAPI

OilPriceAPI is a RESTful providing various endpoints and parameters for retrieving historical or live oil prices.


## OilPriceAPI Handler Implementation

This handler was implemented using the `requests` library that makes http calls to https://docs.oilpriceapi.com/guide/#endpoints.

## OilPriceAPI Handler Initialization

The OilPriceAPI handler is initialized with the following parameters:

- `api_key`: API Key used to authenticate with OilPriceAPI

Read about creating an API Key [here](https://www.oilpriceapi.com/).

## Implemented Features

- [x] OilPriceAPI 
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## Example Usage

The first step is to create a database with the new `oilpriceapi` engine. 

~~~~sql
CREATE DATABASE mindsdb_oilpriceapi
WITH ENGINE = 'oilpriceapi',
PARAMETERS = {
  "api_key": ""
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_oilpriceapi.latest_price;
~~~~

~~~~sql
SELECT * FROM mindsdb_oilpriceapi.latest_price where by_type="daily_average_price" and by_code="WTI_USD";
~~~~

~~~~sql
SELECT * FROM mindsdb_oilpriceapi.past_day_price where by_type="daily_average_price" and by_code="WTI_USD";
~~~~
