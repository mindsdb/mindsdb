# Google Analytics API Integration

This handler integrates with both the [Google Analytics Admin API](https://developers.google.com/analytics/devguides/config/admin/v1)
and the [Google Analytics Data API](https://developers.google.com/analytics/devguides/reporting/data/v1) to provide:

- **Admin API**: Manage conversion events (create, read, update, delete)
- **Data API**: Run analytics reports, access realtime data, and fetch metadata about available dimensions and metrics

## Authentication Methods

This handler supports two authentication methods: **OAuth2** (recommended for user-level access) and **Service Account** (for server-to-server access).

### Method 1: OAuth2 Authentication (Recommended)

OAuth2 allows users to authenticate with their Google account and grant access to their Google Analytics properties without sharing credentials.

#### OAuth2 Setup Steps

1. **Create OAuth2 Credentials in Google Cloud Console**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create or select a project
   - Enable the Google Analytics Admin API and Google Analytics Data API
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Choose "Desktop app" or "Web application" as application type
   - Download the client secrets JSON file

2. **Choose Your OAuth2 Method**:

   **Option A: Direct Refresh Token Method** (Simpler for programmatic access)

   If you already have a refresh token, you can use it directly:

   ~~~~sql
   CREATE DATABASE my_ga
   WITH ENGINE = 'google_analytics',
   parameters = {
       'property_id': '123456789',
       'client_id': 'your-client-id.apps.googleusercontent.com',
       'client_secret': 'your-client-secret',
       'refresh_token': 'your-refresh-token'
   };
   ~~~~

   **Option B: Authorization Code Flow** (Interactive user consent)

   First attempt - this will return an authorization URL:

   ~~~~sql
   CREATE DATABASE my_ga
   WITH ENGINE = 'google_analytics',
   parameters = {
       'property_id': '123456789',
       'credentials_file': '/path/to/client_secrets.json'
   };
   ~~~~

   The error message will contain an authorization URL. Visit this URL in your browser, authorize access, and get the authorization code. Then recreate the database with the code:

   ~~~~sql
   CREATE DATABASE my_ga
   WITH ENGINE = 'google_analytics',
   parameters = {
       'property_id': '123456789',
       'credentials_file': '/path/to/client_secrets.json',
       'code': 'authorization-code-from-oauth-flow'
   };
   ~~~~

#### OAuth2 Connection Parameters

* `property_id`: required, the property id of your Google Analytics website
* `credentials_file`: path to OAuth client secrets JSON file (for authorization code flow)
* `credentials_url`: URL to OAuth client secrets JSON file (alternative to credentials_file)
* `code`: authorization code obtained after user consent (for authorization code flow)
* `client_id`: OAuth client ID (for direct refresh token method)
* `client_secret`: OAuth client secret (for direct refresh token method)
* `refresh_token`: OAuth refresh token (for direct refresh token method)
* `token_uri`: OAuth token URI (optional, defaults to https://oauth2.googleapis.com/token)
* `scopes`: comma-separated OAuth scopes (optional, uses default GA scopes if not provided)

### Method 2: Service Account Authentication

Service accounts are useful for server-to-server communication and automated processes.

#### Service Account Setup Steps

1. **Create Service Account in Google Cloud Console**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create or select a project
   - Enable the Google Analytics Admin API and Google Analytics Data API
   - Go to "IAM & Admin" > "Service Accounts"
   - Create a service account and download the JSON key file

2. **Grant Service Account Access to GA4 Property**:
   - Go to your Google Analytics property
   - Navigate to Admin > Property Access Management
   - Add the service account email (found in the JSON file)
   - Grant appropriate permissions (Viewer, Analyst, or Editor)

3. **Create Connection**:

   ~~~~sql
   CREATE DATABASE my_ga
   WITH ENGINE = 'google_analytics',
   parameters = {
       'property_id': '123456789',
       'credentials_file': '/path/to/service_account.json'
   };
   ~~~~

   Or using JSON directly:

   ~~~~sql
   CREATE DATABASE my_ga
   WITH ENGINE = 'google_analytics',
   parameters = {
       'property_id': '123456789',
       'credentials_json': {
           "type": "service_account",
           "project_id": "your-project",
           "private_key_id": "key-id",
           "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
           "client_email": "service-account@project.iam.gserviceaccount.com",
           "client_id": "1234567890",
           "auth_uri": "https://accounts.google.com/o/oauth2/auth",
           "token_uri": "https://oauth2.googleapis.com/token"
       }
   };
   ~~~~

#### Service Account Connection Parameters

* `property_id`: required, the property id of your Google Analytics website
* `credentials_file`: path to service account JSON file
* `credentials_json`: service account credentials as JSON object

### OAuth2 vs Service Account Comparison

| Feature | OAuth2 | Service Account |
|---------|--------|-----------------|
| **Use Case** | User-level access, interactive applications | Server-to-server, automated processes |
| **Setup Complexity** | Requires user authorization flow | Requires granting access in GA4 admin |
| **Access Level** | User's own GA properties | Specific properties granted access |
| **Token Refresh** | Automatic with refresh token | No refresh needed |
| **Best For** | Personal analytics, user dashboards | Scheduled reports, automated analysis |

## Security

All credentials (OAuth tokens and service account keys) are stored securely using MindsDB's encrypted storage system. Once you provide credentials, they are automatically encrypted and stored in the handler's secure storage. Subsequent connections will use the stored encrypted credentials, so you don't need to provide them again.

## Example: Automate your GA4 Property

To see how the Google Analytics handler is used, let's walk through a simple example to create a model to predict
conversion events and counting method.

## Connect to the Google Analytics API

We start by creating a database to connect to the Google Analytics API.

Before creating a database, you will need to have a Google account and have enabled the Google Analytics Admin API and Data API.
You can choose between OAuth2 (recommended for user access) or Service Account authentication (see Authentication Methods section above).

**Example with Service Account:**

~~~~sql
CREATE DATABASE my_ga
WITH ENGINE = 'google_analytics',
parameters = {
    'credentials_file': '/home/talaat/Downloads/service_account.json',
    'property_id': '<YOUR_PROPERTY_ID>'
};
~~~~

**Example with OAuth2 (refresh token):**

~~~~sql
CREATE DATABASE my_ga
WITH ENGINE = 'google_analytics',
parameters = {
    'property_id': '<YOUR_PROPERTY_ID>',
    'client_id': 'your-client-id.apps.googleusercontent.com',
    'client_secret': 'your-client-secret',
    'refresh_token': 'your-refresh-token'
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