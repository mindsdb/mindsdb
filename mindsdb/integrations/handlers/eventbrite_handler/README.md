# Eventbrite Handler

Eventbrite handler for MindsDB provides interfaces to connect to Eventbrite via APIs into MindsDB. Here is the API documentation: https://www.eventbrite.com/platform/api

## About Eventbrite

Eventbrite is a global self-service ticketing platform for live experiences that allows anyone to create, share, find and attend events that fuel their passions and enrich their lives. From music festivals, marathons, conferences, community rallies and fundraisers, to gaming competitions and air guitar contests. Our mission is to bring the world together through live experiences.

## Eventbrite Handler Implementation

This handler was implemented using the [eventbrite-python](https://github.com/GearPlug/eventbrite-python/tree/main) library.
eventbrite-python is a Python library that wraps Eventbrite API v3.

## Eventbrite Handler Initialization

The Eventbrite handler is initialized with the following parameters:

- `access_token`: API key to use for authentication and have an access to data

Read about creating a Eventbrite API Authentication [here](https://www.eventbrite.com/platform/api?internal_ref=social#/introduction/authentication/1.-get-a-private-token).

## Example Usage

```sql
CREATE DATABASE my_eventbrite_handler
WITH ENGINE = "eventbrite",
PARAMETERS = {
  "access_token": "your access token"
};
```

Use the established connection to query your database:

**For ListEventsTable, you need organization permission to list all of their events. Otherwise, 403 error**

```sql
SELECT * FROM my_eventbrite_handler.userInfoTable
```

```sql
SELECT * FROM my_eventbrite_handler.organizationInfoTable
```

```sql
SELECT * FROM my_eventbrite_handler.categoryInfoTable
```

```sql
SELECT * FROM my_eventbrite_handler.subcategoryInfoTable
```

```sql
SELECT * FROM my_eventbrite_handler.formatInfoTable
```

Run more advanced queries:

```sql
SELECT id, name
  FROM my_eventbrite_handler.categoryInfoTable
  ORDER BY name ASC
  LIMIT 3
```

```sql
SELECT * FROM my_eventbrite_handler.userInfoTable
WHERE event_id= "717926867587";
```

```sql
SELECT * FROM my_eventbrite_handler.listEventsTable
WHERE organization_id = '1871338711793';

```
