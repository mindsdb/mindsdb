# Google Analytics API Integration

This handler integrates with both the [Google Analytics Admin API](https://developers.google.com/analytics/devguides/config/admin/v1)
and the [Google Analytics Data API](https://developers.google.com/analytics/devguides/reporting/data/v1) to provide:

- **Admin API**: Manage conversion events (create, read, update, delete)
- **Data API**: Run analytics reports, access realtime data, and fetch metadata about available dimensions and metrics

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

This creates a database called my_ga. This database provides access to the following tables:

### Available Tables

#### Admin API Tables
- **conversion_events**: Manage conversion events (SELECT, INSERT, UPDATE, DELETE)

#### Data API Tables
- **reports**: Run standard GA4 reports with custom dimensions and metrics (SELECT)
- **realtime_reports**: Access realtime user activity data (SELECT)
- **metadata**: Query available dimensions and metrics for your property (SELECT)

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

---

# Data API Usage

The Google Analytics Data API provides powerful reporting capabilities to query your GA4 analytics data. Below are examples of how to use each table.

## Reports Table

The `reports` table allows you to run customized reports with dimensions and metrics.

### Basic Report Example

Query traffic by country for the last 30 days:

~~~~sql
SELECT country, city, activeUsers, sessions
FROM my_ga.reports
WHERE start_date = '30daysAgo'
  AND end_date = 'today'
ORDER BY activeUsers DESC
LIMIT 100;
~~~~

### Custom Date Range

Query specific date range:

~~~~sql
SELECT date, activeUsers, newUsers, sessions
FROM my_ga.reports
WHERE start_date = '2024-01-01'
  AND end_date = '2024-01-31'
ORDER BY date;
~~~~

### Filtered Report

Query events with a specific event name:

~~~~sql
SELECT date, eventName, eventCount
FROM my_ga.reports
WHERE start_date = '7daysAgo'
  AND end_date = 'today'
  AND dimension_eventName = 'first_open';
~~~~

### Available Date Range Formats
- Relative: `'today'`, `'yesterday'`, `'7daysAgo'`, `'30daysAgo'`, `'90daysAgo'`
- Absolute: `'2024-01-01'` (YYYY-MM-DD format)

### Common Dimensions
- Geographic: `country`, `city`, `region`, `continent`
- Technology: `browser`, `deviceCategory`, `operatingSystem`, `platform`
- User: `newVsReturning`, `userAgeBracket`, `userGender`
- Traffic Source: `source`, `medium`, `campaign`, `sessionSource`
- Content: `pagePath`, `pageTitle`, `landingPage`, `eventName`
- Time: `date`, `dateHour`, `dayOfWeek`, `month`, `year`

### Common Metrics
- Users: `activeUsers`, `newUsers`, `totalUsers`
- Sessions: `sessions`, `sessionsPerUser`, `bounceRate`
- Engagement: `engagementRate`, `userEngagementDuration`, `screenPageViews`
- Events: `eventCount`, `conversions`, `totalRevenue`
- E-commerce: `transactions`, `purchaseRevenue`, `itemsPurchased`

## Realtime Reports Table

The `realtime_reports` table provides access to current user activity (last 30-60 minutes).

### Basic Realtime Report

See current active users by country:

~~~~sql
SELECT country, activeUsers
FROM my_ga.realtime_reports;
~~~~

### Realtime Report with Multiple Dimensions

~~~~sql
SELECT country, city, deviceCategory, activeUsers, screenPageViews
FROM my_ga.realtime_reports
ORDER BY activeUsers DESC
LIMIT 20;
~~~~

### Filtered Realtime Report

See active users in a specific country:

~~~~sql
SELECT city, activeUsers, screenPageViews
FROM my_ga.realtime_reports
WHERE dimension_country = 'United States';
~~~~

## Metadata Table

The `metadata` table returns all available dimensions and metrics for your property, including custom definitions.

### Get All Dimensions and Metrics

~~~~sql
SELECT * FROM my_ga.metadata;
~~~~

### Get Only Dimensions

~~~~sql
SELECT api_name, ui_name, description, custom, category
FROM my_ga.metadata
WHERE type = 'dimension';
~~~~

### Get Only Metrics

~~~~sql
SELECT api_name, ui_name, description, metric_type, custom, category
FROM my_ga.metadata
WHERE type = 'metric';
~~~~

### Find Available Custom Dimensions/Metrics

~~~~sql
SELECT type, api_name, ui_name, description
FROM my_ga.metadata
WHERE custom = true;
~~~~

## Advanced Examples

### Year-over-Year Comparison

Compare traffic between two time periods:

~~~~sql
-- Current period
SELECT 'current' as period, SUM(CAST(activeUsers AS INT)) as total_users
FROM my_ga.reports
WHERE start_date = '30daysAgo'
  AND end_date = 'today';

-- Previous period
SELECT 'previous' as period, SUM(CAST(activeUsers AS INT)) as total_users
FROM my_ga.reports
WHERE start_date = '60daysAgo'
  AND end_date = '31daysAgo';
~~~~

### Top Traffic Sources

~~~~sql
SELECT sessionSource, sessionMedium, activeUsers, sessions, conversions
FROM my_ga.reports
WHERE start_date = '30daysAgo'
  AND end_date = 'today'
ORDER BY activeUsers DESC
LIMIT 10;
~~~~

### Device Category Performance

~~~~sql
SELECT deviceCategory, activeUsers, sessions, bounceRate, conversions
FROM my_ga.reports
WHERE start_date = '7daysAgo'
  AND end_date = 'today'
ORDER BY deviceCategory;
~~~~

### Page Performance Analysis

~~~~sql
SELECT pagePath, pageTitle, screenPageViews, userEngagementDuration, bounceRate
FROM my_ga.reports
WHERE start_date = '30daysAgo'
  AND end_date = 'today'
ORDER BY screenPageViews DESC
LIMIT 20;
~~~~

## Working with Custom Dimensions and Metrics

Google Analytics 4 supports custom dimensions and metrics, which appear in the API with colon syntax (e.g., `customEvent:job_title`, `customUser:subscription_tier`).

### Column Name Sanitization

To make custom dimensions work seamlessly with SQL, colons are automatically replaced with underscores in query results:

- **API Name**: `customEvent:job_title`
- **Column Name in SQL**: `customEvent_job_title`

### Querying Custom Dimensions

Use the sanitized name (with underscores) in your SQL queries:

~~~~sql
SELECT customEvent_job_title, customUser_subscription_tier, activeUsers
FROM my_ga.reports
WHERE start_date = '30daysAgo'
  AND end_date = 'today'
ORDER BY activeUsers DESC
LIMIT 100;
~~~~

### Discovering Custom Dimensions

Use the `metadata` table to see both the original API name and the SQL column name:

~~~~sql
SELECT api_name, column_name, ui_name, description
FROM my_ga.metadata
WHERE custom = true;
~~~~

Example output:
```
| api_name                      | column_name                    | ui_name            | description                      |
|-------------------------------|--------------------------------|--------------------|----------------------------------|
| customEvent:job_title         | customEvent_job_title          | Job Title          | User's job title event parameter |
| customUser:subscription_tier  | customUser_subscription_tier   | Subscription Tier  | User subscription level          |
```

### Filtering by Custom Dimensions

~~~~sql
SELECT date, customEvent_achievement_id, eventCount
FROM my_ga.reports
WHERE start_date = '7daysAgo'
  AND end_date = 'today'
  AND dimension_customEvent_achievement_id = 'level_up';
~~~~

## Best Practices

1. **Always specify date ranges**: The `reports` table requires `start_date` and `end_date` in the WHERE clause.
2. **Use LIMIT**: GA4 API responses can be large. Use LIMIT to control result size.
3. **Check metadata first**: Query the `metadata` table to discover available dimensions and metrics for your property.
4. **Realtime data**: Use `realtime_reports` for monitoring current activity; use `reports` for historical analysis.
5. **Dimension filters**: Use `dimension_<name> = 'value'` in WHERE clause to filter by dimension values.
6. **Custom dimensions**: Use the sanitized column names (underscores instead of colons) when querying custom dimensions.

## Additional Resources

- [GA4 Dimensions & Metrics Reference](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)
- [GA4 Data API Quickstart](https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries)
- [GA4 Realtime Reports](https://developers.google.com/analytics/devguides/reporting/data/v1/realtime-basics)