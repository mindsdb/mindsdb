# HubSpot Handler

HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.

---

## Table of Contents

- [HubSpot Handler](#hubspot-handler)
  - [Table of Contents](#table-of-contents)
  - [About HubSpot](#about-hubspot)
  - [Installation](#installation)
  - [Authentication](#authentication)
    - [Personal Access Token Authentication](#personal-access-token-authentication)
  - [Supported Tables](#supported-tables)
    - [Core CRM and Engagement Tables](#core-crm-and-engagement-tables)
    - [Metadata Tables](#metadata-tables)
    - [Association Tables](#association-tables)
  - [Data Catalog Support](#data-catalog-support)
  - [Example Usage](#example-usage)
    - [Basic Connection](#basic-connection)
    - [Querying Data](#querying-data)
    - [Data Manipulation](#data-manipulation)
  - [Notes on Filters and Limits](#notes-on-filters-and-limits)

---

## About HubSpot

HubSpot is a comprehensive CRM platform providing marketing, sales, content management, and customer service tools. This integration exposes HubSpot CRM data through MindsDB's SQL interface.

**Official Website:** https://www.hubspot.com/products
**API Documentation:** https://developers.hubspot.com/docs/api/overview

## Installation

Install the handler dependencies using pip:

```bash
pip install -r requirements.txt
```

**Required Dependencies:**
- `hubspot-api-client==12.0.0` - Official HubSpot Python client

## Authentication

The handler supports two authentication methods:

### Personal Access Token Authentication

Recommended for server-to-server integrations and production environments.

**Steps to obtain an access token:**
1. Navigate to your HubSpot account settings
2. Go to Integrations -> Private Apps
3. Create a new private app or select an existing one
4. Configure required scopes for the tables you plan to access
5. Copy the generated access token


## Supported Tables

### Core CRM and Engagement Tables

These tables support `SELECT`, `INSERT`, `UPDATE`, and `DELETE` operations.

| Table Name | Description | Reference |
|------------|-------------|-------------|
| `companies` | Company records from HubSpot CRM | https://developers.hubspot.com/docs/api-reference/crm-companies-v3/guide |
| `contacts` | Contact records from HubSpot CRM | https://developers.hubspot.com/docs/api-reference/crm-contacts-v3/guide |
| `deals` | Deal records from HubSpot CRM | https://developers.hubspot.com/docs/api-reference/crm-deals-v3/guide |
| `tickets` | Support ticket records | https://developers.hubspot.com/docs/api-reference/crm-tickets-v3/guide |
| `tasks` | Task and follow-up records | https://developers.hubspot.com/docs/api-reference/crm-tasks-v3/guide |
| `calls` | Call log records |  https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide |
| `emails` | Email log records | https://developers.hubspot.com/docs/api-reference/crm-emails-v3/guide |
| `meetings` | Meeting records | https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide |
| `notes` | Timeline notes | https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide |

### Metadata Tables

These tables are read-only and support `SELECT` only.

| Table Name | Description | Reference |
|------------|-------------|-------------|
| `owners` | HubSpot owners with names and emails | https://developers.hubspot.com/docs/api-reference/crm-owners-v3/guide |
| `pipelines` | Deal pipelines with names and stages | https://developers.hubspot.com/docs/api-reference/crm-pipelines-v3/guide |

### Association Tables

Association tables are read-only and support `SELECT` only. They expose relationships between objects and include `association_type` and `association_label` columns. 
 
 Reference: https://developers.hubspot.com/docs/api-reference/crm-associations-v4/guide

| Table Name | Description |
|------------|-------------|
| `company_contacts` | Company to contact associations |
| `company_deals` | Company to deal associations |
| `company_tickets` | Company to ticket associations |
| `contact_companies` | Contact to company associations |
| `contact_deals` | Contact to deal associations |
| `contact_tickets` | Contact to ticket associations |
| `deal_companies` | Deal to company associations |
| `deal_contacts` | Deal to contact associations |
| `ticket_companies` | Ticket to company associations |
| `ticket_contacts` | Ticket to contact associations |
| `ticket_deals` | Ticket to deal associations |

## Data Catalog Support

The handler provides `SHOW TABLES` and `information_schema.columns` support for all tables. Column statistics are sampled for core CRM and engagement tables.

## Example Usage

### Basic Connection

**Using Access Token:**
```sql
CREATE DATABASE hubspot_datasource
WITH ENGINE = 'hubspot',
PARAMETERS = {
  "access_token": "pat-na1-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
};
```

**Using OAuth (Advanced):**
```sql
CREATE DATABASE hubspot_datasource
WITH ENGINE = 'hubspot',
PARAMETERS = {
  "client_id": "your-client-id",
  "client_secret": "your-client-secret"
};
```

### Querying Data

**Basic Data Retrieval:**
```sql
SELECT * FROM hubspot_datasource.companies LIMIT 10;
SELECT * FROM hubspot_datasource.contacts LIMIT 10;
SELECT * FROM hubspot_datasource.deals LIMIT 10;
```

**Date Filters (Supported Functions):**
```sql
SELECT * FROM hubspot_datasource.deals
WHERE closedate >= DATE_SUB(CURRENT_DATE, INTERVAL 2 YEAR);
```

### Data Manipulation

**Creating Records:**
```sql
INSERT INTO hubspot_datasource.companies (name, domain, industry, city, state)
VALUES ('Acme Corp', 'acme.com', 'COMPUTER_SOFTWARE', 'New York', 'NY');

INSERT INTO hubspot_datasource.contacts (email, firstname, phone)
VALUES ('john.doe@example.com', 'John', '+1234567890');

INSERT INTO hubspot_datasource.tasks (hs_task_subject, hs_task_status)
VALUES ('Follow up with Acme', 'WAITING');
```

**Updating Records:**
```sql
UPDATE hubspot_datasource.companies
SET industry = 'COMPUTER_SOFTWARE', city = 'Austin'
WHERE name = 'Acme Corp';

UPDATE hubspot_datasource.deals
SET dealstage = '110382973', amount = '75000'
WHERE dealname = 'New Deal';
```

**Deleting Records:**
```sql
DELETE FROM hubspot_datasource.deals
WHERE dealstage = 'closedlost'
  AND createdate < '2023-01-01';
```

## Notes on Filters and Limits

- Supported filter operators include `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, and `NOT IN`.
- Date helpers supported in filters include `CURDATE()`/`CURRENT_DATE`, `NOW()`/`CURRENT_TIMESTAMP`, `DATE_SUB`, and `DATE_ADD`.
- Updates and deletes evaluate conditions against a sample of up to 200 records before applying changes.
- Unsupported filters or order-by expressions are skipped rather than raising errors.
