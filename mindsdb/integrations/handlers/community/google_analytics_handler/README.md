# Google Analytics API Integration

This handler integrates with the [Google Analytics Admin API](https://developers.google.com/analytics/devguides/config/admin/v1)
to make conversion events data available to use for model training and predictions.

## Parameters
* `property_id`: required, the property id of your Google Analytics website
* `credentials_file`: optional, full path to the credentials file (Service Account Credentials)
* `credentials_json`: optional, credentials file content as json (Service Account Credentials)
> ⚠️ One of credentials_file or credentials_json has to be chosen.

## Example: Automate your GA4 Property

To see how the Google Analytics handler is used, let's walk through a simple example to create a model to predict
conversion events and counting method.

## Connect to the Google Analytics API

We start by creating a database to connect to the Google Analytics API.

Before creating a database, you will need to have a Google account and have enabled the Google Analytics Admin API.
Also, you will need to have a Google Analytics account created in your Google account and the credentials for that GA property
in a json file. You can find more information on how to do
this [here](https://developers.google.com/analytics/devguides/config/admin/v1/quickstart-client-libraries).

~~~~sql
CREATE
DATABASE my_ga
WITH  ENGINE = 'google_analytics',
parameters = {
    'credentials_file': '/home/talaat/Downloads/credentials.json',
    'property_id': '<YOUR_PROPERTY_ID>'
};    
~~~~

This creates a database called my_ga. This database ships with a table called conversion_events that we can use to search for
conversion events as well as to process them.

## Searching for conversion events in SQL

Let's get a list of conversion events in our GA property.

~~~~sql
SELECT event_name,
       deletable,
       custom,
       countingMethod
FROM my_ga.conversion_events;
~~~~

## Creating Conversion Event using SQL

Let's test by creating a conversion event and set the counting method in our GA property.

~~~~sql
INSERT INTO my_ga.conversion_events (event_name, countingMethod)
VALUES ('mindsdb_event', 2);
~~~~
`countingMethod = 2` Means that counting method is `ONCE_PER_SESSION`<br/>
`countingMethod = 1` Means that counting method is `ONCE_PER_EVENT`
## Updating Conversion Events using SQL

Let's update the conversion event we just created.

~~~~sql
UPDATE my_ga.conversion_events
SET countingMethod = 1
WHERE name = '<NAME_OF_YOUR_EVENT>';
~~~~
For `conversion_events` table you can only update the counting method.<br />
`name` Is the name of the conversion event we just created.

## Deleting Events using SQL

Let's delete the conversion event we just created.

~~~~sql
DELETE
FROM my_ga.conversion_events
WHERE name = '<NAME_OF_YOUR_EVENT>'
~~~~

## Creating a model to recommend conversion events

Now that we have some data in our Google Analytics DB, we can create recommendations for available conversion events, and other automations.

~~~~sql
CREATE
PREDICTOR predict_conversion_events
FROM my_ga.conversion_events
PREDICT event_name, countingMethod
~~~~