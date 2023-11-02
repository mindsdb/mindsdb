# World Air Quality Index Handler

World Air Quality Index handler for MindsDB provides interfaces to connect to [World Air Quality Index](https://aqicn.org) via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [World Air Quality Index Handler](#world-air-quality-index-handler)
  - [Table of Contents](#table-of-contents)
  - [About World Air Quality Index Handler](#about-world-air-quality-index-handler)
  - [World Air Quality Index Handler Implementation](#world-air-quality-index-handler-implementation)
  - [World Air Quality Index Handler Initialization](#world-air-quality-index-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About World Air Quality Index Handler

The World Air Quality Index project is a non-profit project started in 2007. Its mission is to promote air pollution awareness for citizens and provide a unified and world-wide air quality information.

The project is providing transparent air quality information for more than 130 countries, covering more than 30,000 stations in 2000 major cities, via those two websites: aqicn.org and waqi.info


## World Air Quality Index Handler Implementation

This handler was implemented using the `requests` library that makes http calls to https://aqicn.org/json-api/doc/

## World Air Quality Index Handler Initialization

The World Air Quality Index handler is initialized with the following parameters:

- `api_key`: API key to interact with aqicn

Read about creating an account [here](https://aqicn.org/api/).

## Implemented Features

- [x] World Air Quality Index data for lattitude and longitude
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection


## Example Usage

The first step is to create a database with the new `aqicn` engine. 

~~~~sql
CREATE DATABASE mindsdb_aqicn
WITH ENGINE = 'aqicn',
PARAMETERS = {
  "api_key": "api_key"
};
~~~~

Use the established connection to query your database:

To get air quality metrics based on your location:

~~~~sql
SELECT * FROM mindsdb_aqicn.air_quality_user_location;
~~~~

To get air quality metrics based on city:

~~~~sql
SELECT * FROM mindsdb_aqicn.air_quality_city where city="Bangalore";
~~~~

The `city` column is mandatory in the above query.

To get air quality metrics based on coordinates:

~~~~sql
SELECT * FROM mindsdb_aqicn.air_quality_lat_lng where lat="12.938539" AND lng="77.5901";
~~~~

The `lat` and `lng` columns are mandatory in the above query.

To get air quality metrics based on station name:

~~~~sql
SELECT * FROM mindsdb_aqicn.air_quality_station_by_name where name="bangalore";
~~~~

The `name` column is mandatory in the above query.
