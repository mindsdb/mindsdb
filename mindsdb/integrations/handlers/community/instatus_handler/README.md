# Instatus Handler

Instatus handler for MindsDB provides interfaces to connect with Instatus via APIs and pull the status pages.

## Instatus

Instatus is a cloud-based status page software that allows users to communicate their status using incidents and maintenances. It's a SaaS platform that helps companies create status pages for their services.

## Instatus Handler Initialization

The Instatus handler is initialized with the following parameters:

- `api_key`: Instatus API key to use for authentication

Please follow this [link](https://dashboard.instatus.com/developer) to get the api key for accessing Instatus API.

## Implemented Features

- [x] Instatus status pages table
  - [x] Support SELECT
  - [x] Support INSERT
  - [x] Support UPDATE
- [x] Instatus components table
  - [x] Support SELECT
  - [x] Support INSERT
  - [x] Support UPDATE

## TODO
- [ ] Instatus Incidents table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Incidents updates table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Maintenances table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Maintenance updates table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Templates table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Teammates table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Subscribers table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE
- [ ] Instatus Metrics table
  - [ ] Support SELECT
  - [ ] Support INSERT
  - [ ] Support UPDATE

## Connection

The first step is to create a database with the new `instatus` engine.

```sql
CREATE DATABASE mindsdb_instatus --- Display name for the database.
WITH
  ENGINE = 'instatus', --- Name of the MindsDB handler.
  PARAMETERS = {
    "api_key": "<your-instatus-api-key>" --- Instatus API key to use for authentication.
  };
```
## Usage (Status pages table)

### SELECT

```sql
SELECT *
FROM mindsdb_instatus.status_pages;
```

### WHERE

```sql
SELECT *
FROM mindsdb_instatus.status_pages
WHERE id = '<status-page-id>';
```

### INSERT

```sql
INSERT INTO mindsdb_instatus.status_pages (email, name, subdomain, components, logoUrl, faviconUrl, websiteUrl, language, useLargeHeader, brandColor, okColor, disruptedColor, degradedColor, downColor, noticeColor, unknownColor, googleAnalytics, subscribeBySms, smsService, twilioSid, twilioToken, twilioSender, nexmoKey, nexmoSecret, nexmoSender, htmlInMeta, htmlAboveHeader, htmlBelowHeader, htmlAboveFooter, htmlBelowFooter, htmlBelowSummary, cssGlobal, launchDate, dateFormat, dateFormatShort, timeFormat)
VALUES ('yourname@gmail.com', 'mindsdb', 'mindsdb-instatus', '["Website", "App", "API"]', 'https://instatus.com/sample.png', 'https://instatus.com/favicon-32x32.png', 'https://instatus.com', 'en', true, '#111', '#33B17E', '#FF8C03', '#ECC94B', '#DC123D', '#70808F', '#DFE0E1', 'UA-00000000-1', true, 'twilio', 'YOUR_TWILIO_SID', 'YOUR_TWILIO_TOKEN', 'YOUR_TWILIO_SENDER', null, null, null, null, null, null, null, null, null, null, null, 'MMMMMM d, yyyy', 'MMM yyyy', 'p');
```

Note:

- `email` is required field (Example: 'yourname@gmail.com')
- `name` is required field (Example: 'mindsdb')
- `subdomain` is required field (Example: 'mindsdb-docs')
- `components` is required field (Example: '["Website", "App", "API"]')
- other fields are optional

### UPDATE

```sql
UPDATE mindsdb_instatus.status_pages
SET name = 'mindsdb',
    status = 'UP',
    subdomain = 'mindsdb-slack',
    logoUrl = 'https://instatus.com/sample.png',
    faviconUrl = 'https://instatus.com/favicon-32x32.png',
    websiteUrl = 'https://instatus.com',
    language = 'en',
    publicEmail = 'hello@nasa.gov',
    useLargeHeader = true,
    brandColor = '#111',
    okColor = '#33B17E',
    disruptedColor = '#FF8C03',
    degradedColor = '#ECC94B',
    downColor = '#DC123D',
    noticeColor = '#70808F',
    unknownColor = '#DFE0E1',
    googleAnalytics = 'UA-00000000-1',
    subscribeBySms = true,
    smsService = 'twilio',
    twilioSid = 'YOUR_TWILIO_SID',
    twilioToken = 'YOUR_TWILIO_TOKEN',
    twilioSender = 'YOUR_TWILIO_SENDER',
    nexmoKey = null,
    nexmoSecret = null,
    nexmoSender = null,
    htmlInMeta = null,
    htmlAboveHeader = null,
    htmlBelowHeader = null,
    htmlAboveFooter = null,
    htmlBelowFooter = null,
    htmlBelowSummary = null,
    cssGlobal = null,
    launchDate = null,
    dateFormat = 'MMMMMM d, yyyy',
    dateFormatShort = 'MMM yyyy',
    timeFormat = 'p',
    private = false,
    useAllowList = false,
    translations = '{
      "name": {
        "fr": "nasa"
      }
    }'
WHERE id = '<status-page-id>';
```

## Usage (Components table)

### SELECT

```sql
SELECT *
FROM mindsdb_instatus.components
WHERE page_id = '<status-page-id>';
```

### WHERE

```sql
SELECT *
FROM mindsdb_instatus.components
WHERE page_id = '<status-page-id>'
AND component_id = '<component-id>';
```

### CREATE

```sql
INSERT INTO mindsdb_instatus.components (page_id, name, description, status, order, showUptime, grouped, translations_name_in_fr, translations_desc_in_fr)
VALUES (
  '<page-id>',
  'Test component',
  'Testing',
  'OPERATIONAL',
  6,
  true,
  false,
  "Composant de test",
  "En test"
);
```

### UPDATE

```sql
UPDATE mindsdb_instatus.components
SET
  name = 'Test component 4',
  description = 'Test test test',
  status = 'OPERATIONAL',
  order = 6,
  showUptime = true,
  grouped = false,
  translations_name_in_fr = "Composant de test 4",
  translations_desc_in_fr = "Test test test"
WHERE page_id = '<status-page-id>'
AND component_id = '<component-id>';
```
