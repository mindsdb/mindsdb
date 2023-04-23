# Strava Handler
Strava handler for MindsDB provides interfaces to connect with strava via APIs and pull the workout data of your fitness club into MindsDB.

## Strava
Strava is app used for tracking physical exercise and share the data with your social network 

## Strava Handler Initialization

The Strava handler is initialized with the following parameters:

- `strava_api_token`: Strava API key to use for authentication 

Please follow this (link)[https://developers.strava.com/docs/getting-started/] to generate the token for accessing strava API

## Implemented Features

- [x] Strava all_clubs table 
  - [x] Support LIMIT
  - [x] Support ORDER BY
  - [x] Support column selection

- [x] Strava club_activities table 
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection


## Example Usage

The first step is to create a database with the new `strava` engine.

~~~~sql
CREATE DATABASE mindsdb_strava
WITH ENGINE = 'strava',
PARAMETERS = {
  "strava_api_token": "ccf04d25ed81a5df8632afb11fdf902691e2dfb3"  
};
~~~~

Use the established connection to query the Strava all_clubs table 

~~~~sql
SELECT * FROM mindsdb_strava.all_clubs;
~~~~

Use the established connection to query the Strava club_activities table 

~~~~sql
SELECT * FROM mindsdb_strava.club_activities
WHERE strava_club_id = 158786;
~~~~


Advanced queries for the Strava handler

~~~~sql
SELECT id,localized_sport_type,country,member_count FROM 
mindsdb_strava.all_clubs
ORDER by id ASC
LIMIT 10;
~~~~~~~

~~~~sql
SELECT distance, sport_type, athlete_firstname
FROM
mindsdb_strava.club_activities
WHERE strava_club_id = 158786;
ORDER BY athelete.firstname ASC
LIMIT 10
~~~~
