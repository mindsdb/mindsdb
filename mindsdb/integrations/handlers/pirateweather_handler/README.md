# PirateWeather Handler

The PirateWeather handler allows you to query historical weather data from [PirateWeather](http://pirateweather.net/).

## PirateWeather Handler Setup

The PirateWeather handler is initialized with the following parameters:

- `api_key`: your PirateWeather API key to use for authentication

Read about creating a PirateWeather API key [here](http://pirateweather.net/en/latest/).

Provided Tables

- `hourly` - historical hourly weather data for a given location. Columns:
    - `localtime`
    - `icon`
    - `summary`
    - `precipAccumulation`
    - `precipType`
    - `temperature`
    - `apparentTemperature`
    - `dewPoint`
    - `pressure`
    - `windSpeed`
    - `windBearing`
    - `cloudCover`
    - `latitude`
    - `longitude`
    - `timezone`
    - `offset`
- `daily` - historical daily weather data for a given location. Columns:
    - `localtime`
    - `icon`
    - `summary`
    - `precipAccumulation`
    - `precipType`
    - `temperature`
    - `apparentTemperature`
    - `dewPoint`
    - `pressure`
    - `windSpeed`
    - `windBearing`
    - `cloudCover`
    - `latitude`
    - `longitude`
    - `timezone`
    - `offset`

See [here](http://pirateweather.net/en/latest/API/#time-machine-request) for more information.

Both tables support the following parameters:

* `latitude` - latitude of the location. Required.
* `longitude` - longitude of the location. Required.
* `time` - Date for which to fetch historical data. Optional, defaults to the current date.
* `units` - Units to use for temperature and wind speed. Optional, defaults to `us` (Imperial units). Other options are:
    * `ca`: SI, with Wind Speed and Wind Gust in kilometres per hour.
    * `uk`: SI, with Wind Speed and Wind Gust in miles per hour and visibility are in miles.
    * `us`: Imperial units
    * `si`: SI units

## Example Usage

The first step is to create a database with the new `pirateweather` engine.

~~~~sql
CREATE
DATABASE pirateweather
WITH ENGINE = 'pirateweather',
PARAMETERS = {
  "api_key": "your_api_key"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT *
FROM pirateweather.hourly
WHERE latitude = 51.507351
  AND longitude = -0.127758
  AND time ="1672578052"
~~~~

~~~~sql
SELECT *
FROM pirateweather.daily
WHERE latitude = 51.507351
  AND longitude = -0.127758
  AND time ="1672578052"
~~~~

You can further query the returned data as usual:

~~~~sql
SELECT *
FROM pirateweather.daily
WHERE latitude = 51.507351
  AND longitude = -0.127758
  AND time ="1672578052"
  AND temperature
    > 50
ORDER BY temperature DESC
    LIMIT 10
~~~~


