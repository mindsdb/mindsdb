# HubSpot Handler

HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.

---

## Table of Contents

- [HubSpot Handler](#hubspot-handler)
  - [About HubSpot](#about-hubspot)
  - [Installation](#installation)
  - [Authentication](#authentication)
    - [Access Token Authentication](#access-token-authentication)
    - [OAuth Authentication](#oauth-authentication)
  - [Data Catalog Support](#data-catalog-support)
  - [Available Tables](#available-tables)
  - [Example Usage](#example-usage)
    - [Basic Connection](#basic-connection)
    - [Data Catalog Operations](#data-catalog-operations)
    - [Querying Data](#querying-data)
    - [Data Manipulation](#data-manipulation)

---

## About HubSpot

HubSpot is a comprehensive CRM platform providing marketing, sales, content management, and customer service tools. This integration provides secure, enterprise-ready access to HubSpot's CRM data through MindsDB's unified interface.

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

The handler supports two authentication methods with enterprise-grade security:

### Access Token Authentication

Recommended for server-to-server integrations and production environments.

**Steps to obtain access token:**
1. Navigate to your HubSpot account settings
2. Go to Integrations → Private Apps
3. Create a new private app or select existing one
4. Configure required scopes (contacts, companies, deals)
5. Copy the generated access token

**Security Note:** Access tokens provide full API access. Store securely and rotate regularly.

### OAuth Authentication

Recommended for applications requiring user consent and dynamic scope management.

**Required OAuth Parameters:**
- `client_id`: Your app's client identifier
- `client_secret`: Your app's client secret (store securely)
- OAuth flow implementation (handled externally)

**Security Note:** Never expose client secrets in client-side code. Use server-side token exchange.


## Data Catalog Support

The handler provides comprehensive data catalog capabilities:

**Table Metadata:**
- `TABLE_NAME`: Name of the table (companies, contacts, deals)
- `TABLE_TYPE`: Always "BASE TABLE" for HubSpot entities
- `TABLE_SCHEMA`: Schema identifier ("hubspot")
- `TABLE_DESCRIPTION`: Human-readable description of table contents
- `ROW_COUNT`: Estimated number of records (when available)

**Column Metadata:**
- `COLUMN_NAME`: Column identifier
- `DATA_TYPE`: SQL data type (VARCHAR, INTEGER, TIMESTAMP, etc.)
- `IS_NULLABLE`: Whether column accepts NULL values
- `COLUMN_DEFAULT`: Default value (if any)
- `COLUMN_DESCRIPTION`: Column purpose and HubSpot property mapping


## Available Tables

| Table Name | Description | Key Columns | Primary Operations |
|------------|-------------|-------------|-------------------|
| `companies` | Organization records from HubSpot CRM | id, name, domain, industry | SELECT, INSERT, UPDATE, DELETE |
| `contacts` | Individual contact records | id, email, firstname, lastname | SELECT, INSERT, UPDATE, DELETE |
| `deals` | Sales opportunity records | id, dealname, amount, stage | SELECT, INSERT, UPDATE, DELETE |

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

### Data Catalog Operations

**List Available Tables:**
```sql
SHOW TABLES FROM hubspot_datasource;
```

**Get Table Schema:**
```sql
DESCRIBE hubspot_datasource.companies;
DESCRIBE hubspot_datasource.contacts;  
DESCRIBE hubspot_datasource.deals;
```

**Get Detailed Column Information:**
```sql
SELECT * FROM information_schema.columns 
WHERE table_schema = 'hubspot_datasource' 
AND table_name = 'companies';
```

### Querying Data

**Basic Data Retrieval:**
```sql
-- Get all companies
SELECT * FROM hubspot_datasource.companies LIMIT 10;

-- Get all contacts  
SELECT * FROM hubspot_datasource.contacts LIMIT 10;

-- Get all deals
SELECT * FROM hubspot_datasource.deals LIMIT 10;
```

**Advanced Filtering and Analytics:**
```sql
-- Companies by industry and location
SELECT name, industry, city, state
FROM hubspot_datasource.companies
WHERE industry IN ('Technology', 'Healthcare')
  AND city = 'San Francisco'
ORDER BY name;

-- Contact engagement analysis
SELECT 
    company,
    COUNT(*) as contact_count,
    STRING_AGG(email, ', ') as emails
FROM hubspot_datasource.contacts
WHERE company IS NOT NULL
GROUP BY company
ORDER BY contact_count DESC;

-- Sales pipeline analysis
SELECT 
    dealstage,
    COUNT(*) as deal_count,
    SUM(CAST(amount AS DECIMAL)) as total_value,
    AVG(CAST(amount AS DECIMAL)) as avg_deal_size
FROM hubspot_datasource.deals  
WHERE amount IS NOT NULL
GROUP BY dealstage
ORDER BY total_value DESC;
```

### Data Manipulation

**Creating Records:**
```sql
-- Create new company
INSERT INTO hubspot_datasource.companies (name, domain, industry, city, state)
VALUES ('Acme Corp', 'acme.com', 'Technology', 'New York', 'NY');

-- Create new contact  
INSERT INTO hubspot_datasource.contacts (email, firstname, lastname, company, phone)
VALUES ('john.doe@acme.com', 'John', 'Doe', 'Acme Corp', '+1-555-0123');

-- Create new deal
INSERT INTO hubspot_datasource.deals (dealname, amount, pipeline, dealstage)
VALUES ('Acme Software License', '50000', 'sales', 'qualified-to-buy');
```

**Updating Records:**
```sql
-- Update company information
UPDATE hubspot_datasource.companies 
SET industry = 'SaaS', city = 'Austin'
WHERE name = 'Acme Corp';

-- Update contact details
UPDATE hubspot_datasource.contacts
SET phone = '+1-555-9999', company = 'Acme Corporation'  
WHERE email = 'john.doe@acme.com';

-- Move deal through pipeline
UPDATE hubspot_datasource.deals
SET dealstage = 'proposal-made', amount = '75000'
WHERE dealname = 'Acme Software License';
```

**Deleting Records:**
```sql  
-- Archive old deals
DELETE FROM hubspot_datasource.deals  
WHERE dealstage = 'closed-lost' 
  AND createdate < '2023-01-01';

-- Remove test contacts
DELETE FROM hubspot_datasource.contacts
WHERE email LIKE '%test%' OR email LIKE '%example%';
```
