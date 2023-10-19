# Google Calendar API Integration

This handler integrates with the [Google Calendar API](https://developers.google.com/calendar/api/guides/overview)
to make event data available to use for model training and predictions.

## Example: Automate your Calendar

To see how the Google Calendar handler is used, let's walk through a simple example to create a model to predict
future events in your calendar.

## Connect to the Google Calendar API

We start by creating a database to connect to the Google Calendar API. Currently, there is no need for an API key:

However, you will need to have a Google account and have enabled the Google Calendar API.
Also, you will need to have a calendar created in your Google account and the credentials for that calendar
in a json file. You can find more information on how to do
this [here](https://developers.google.com/calendar/quickstart/python).

**Optional:**  The credentials file can be stored in the google_calendar handler folder in
the [mindsdb/integrations/google_calendar_handler](mindsdb/integrations/handlers/google_calendar_handler) directory.

~~~~sql
CREATE
DATABASE my_calendar
WITH  ENGINE = 'google_calendar',
parameters = {
    'credentials': 'C:\\Users\\panagiotis\\Desktop\\GitHub\\mindsdb\\mindsdb\\integrations\\handlers\\google_calendar_handler\\credentials.json'
};    
~~~~

This creates a database called my_calendar. This database ships with a table called events that we can use to search for
events as well as to process events.

## Searching for events in SQL

Let's get a list of events in our calendar.

~~~~sql
SELECT id,
       created,
       creator,
       summary
FROM my_calendar.events
WHERE start_time > '2023-02-16'
  AND end_time < '2023-04-09' LIMIT 20;
~~~~

## Creating Events using SQL

Let's test by creating an event in our calendar.

~~~~sql
INSERT INTO my_calendar.calendar (start_time, end_time, summary, description, location, attendees, reminders)
VALUES ('2023-02-16 10:00:00', '2023-02-16 11:00:00', 'MindsDB Meeting', 'Discussing the future of MindsDB',
        'MindsDB HQ', '')

~~~~

## Updating Events using SQL

Let's update the event we just created.

~~~~sql
UPDATE my_calendar.events
SET summary     = 'MindsDB Meeting',
    description = 'Discussing the future of MindsDB',
    location    = 'MindsDB HQ',
    attendees   = '',
    reminders   = ''
~~~~

Or you can update all events in a given id range.

~~~~sql
UPDATE my_calendar.events
SET summary     = 'MindsDB Meeting',
    description = 'Discussing the future of MindsDB',
    location    = 'MindsDB HQ',
    attendees   = '',
    reminders   = ''
WHERE event_id > 1
  AND event_id < 10
~~~~

If you have specified only one aspect of the comparison (`>` or `<`), then the `start_id` will be `end_id` - 10 (
if `start_id` is
not defined) and the `end_id` will be `start_id` + 10 (if `end_id` is defined).

## Deleting Events using SQL

Let's delete the event we just created.

~~~~sql
DELETE
FROM my_calendar.events
WHERE id = '1'
~~~~

Or you can delete all events in a given id range.

~~~~sql
DELETE
FROM my_calendar.events
WHERE event_id > 1
  AND event_id < 10
~~~~

If you have specified only one aspect of the comparison (`>` or `<`), then the `start_id` will be `end_id` - 10 (
if `start_id` is
not defined) and the `end_id` will be `start_id` + 10 (if `end_id` is defined).

## Creating a model to predict future events

Now that we have some data in our calendar, we can do smarter scheduling, event recommendations, and other automations.

~~~~sql
CREATE
PREDICTOR predict_future_events
FROM my_calendar.events
PREDICT start_time, end_time, summary, description, location, attendees, reminders
WHERE timeMin = '2023-02-16'
  AND timeMax = '2023-04-09'
~~~~