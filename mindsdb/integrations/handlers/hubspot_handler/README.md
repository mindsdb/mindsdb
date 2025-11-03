# HubSpot Handler

HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.
---

## Table of Contents

- [HubSpot Handler](#hubspot-handler)
  - [HubSpot handler for MindsDB provides interfaces to connect to HubSpot via APIs and pull store data into MindsDB.](#hubspot-handler-for-mindsdb-provides-interfaces-to-connect-to-hubspot-via-apis-and-pull-store-data-into-mindsdb)
  - [Table of Contents](#table-of-contents)
  - [About HubSpot](#about-hubspot)
  - [HubSpot Handler Implementation](#hubspot-handler-implementation)
  - [Installation](#installation)
  - [Authentication](#authentication)
    - [Access Token Authentication](#access-token-authentication)
    - [OAuth Authentication](#oauth-authentication)
  - [Enterprise Features](#enterprise-features)
  - [Data Catalog Support](#data-catalog-support)
  - [Implemented Features](#implemented-features)
    - [🏢 Companies Table](#-companies-table)
    - [👤 Contacts Table](#-contacts-table)
    - [💼 Deals Table](#-deals-table)
  - [Available Tables](#available-tables)
  - [Example Usage](#example-usage)
    - [Basic Connection](#basic-connection)
    - [Data Catalog Operations](#data-catalog-operations)
    - [Querying Data](#querying-data)
    - [Data Manipulation](#data-manipulation)
  - [Error Handling](#error-handling)
  - [Security](#security)
  - [Performance](#performance)
  - [Troubleshooting](#troubleshooting)
    - [Common Issues](#common-issues)
    - [Debugging Steps](#debugging-steps)
    - [Performance Tuning](#performance-tuning)
  - [Known Limitations](#known-limitations)
  - [API Coverage](#api-coverage)

---

## About HubSpot

HubSpot is a comprehensive CRM platform providing marketing, sales, content management, and customer service tools. This integration provides secure, enterprise-ready access to HubSpot's CRM data through MindsDB's unified interface.

**Official Website:** https://www.hubspot.com/products
**API Documentation:** https://developers.hubspot.com/docs/api/overview

## HubSpot Handler Implementation

This enterprise-ready handler is implemented using the official [HubSpot API Client](https://github.com/HubSpot/hubspot-api-python) with comprehensive enhancements for:

- **Enterprise Security**: Secure credential handling with no sensitive data in logs
- **Data Catalog**: Complete metadata support including table/column descriptions and statistics
- **Error Handling**: Comprehensive error handling with detailed logging and recovery
- **Performance**: Optimized API calls with intelligent caching and batching
- **Standards Compliance**: Full PEP8 compliance with comprehensive type hints

## Installation

Install the handler dependencies using pip:

```bash
pip install -r requirements.txt
```

**Required Dependencies:**
- `hubspot-api-client==12.0.0` - Official HubSpot Python client
- `pandas>=1.3.0` - Data manipulation and analysis
- `typing-extensions>=4.0.0` - Enhanced type hints support

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

## Enterprise Features

This integration meets enterprise-ready standards with:

✅ **Data Catalog Support**
- Complete table and column metadata
- Column data types and descriptions  
- Estimated row counts and statistics
- Primary key identification

✅ **Security & Compliance**
- Secure credential storage and handling
- No sensitive data in application logs
- Comprehensive error handling
- Input validation and sanitization

✅ **Performance & Reliability**
- Connection pooling and reuse
- Intelligent API rate limiting
- Automatic retry mechanisms
- Comprehensive logging and monitoring

✅ **API Compatibility**
- SQL API support
- REST API support  
- Python SDK support
- MCP (Model Context Protocol) support

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

## Implemented Features

### 🏢 Companies Table
- ✅ **SELECT Operations**: Full query support with LIMIT, WHERE, ORDER BY
- ✅ **INSERT Operations**: Create new companies with property validation
- ✅ **UPDATE Operations**: Modify existing company records
- ✅ **DELETE Operations**: Archive company records
- ✅ **Column Support**: name, domain, industry, city, state, phone, etc.

### 👤 Contacts Table  
- ✅ **SELECT Operations**: Full query support with filtering and sorting
- ✅ **INSERT Operations**: Create contacts with email validation
- ✅ **UPDATE Operations**: Modify contact information
- ✅ **DELETE Operations**: Archive contact records
- ✅ **Column Support**: email, firstname, lastname, phone, company, website, etc.

### 💼 Deals Table
- ✅ **SELECT Operations**: Complete deal pipeline access
- ✅ **INSERT Operations**: Create deals with amount and stage validation
- ✅ **UPDATE Operations**: Move deals through pipeline stages
- ✅ **DELETE Operations**: Archive completed/cancelled deals
- ✅ **Column Support**: dealname, amount, pipeline, stage, closedate, owner, etc.

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

## Error Handling

The handler provides comprehensive error handling:

**Connection Errors:**
- Invalid credentials: Clear error messages with resolution steps
- Network timeouts: Automatic retry with exponential backoff
- API rate limits: Intelligent throttling and queuing

**Query Errors:**
- Malformed SQL: Detailed syntax error messages
- Invalid table/column names: Suggestions for correct names
- Data validation: Field-level validation with specific requirements

**Data Errors:**
- Missing required fields: List of mandatory columns for operations
- Invalid data types: Automatic type conversion where possible
- Constraint violations: Clear violation descriptions with examples

## Security

**Credential Protection:**
- Credentials stored securely in MindsDB vault
- No sensitive data in application logs
- Automatic credential masking in error messages
- Support for environment variable configuration

**API Security:**
- HTTPS-only connections to HubSpot
- Proper SSL certificate validation
- Request signing and authentication
- Scope-limited access tokens

## Performance

**Optimization Features:**
- Connection pooling and reuse
- Intelligent query batching  
- Automatic pagination for large result sets
- Selective column fetching to minimize data transfer

**Monitoring:**
- Request/response timing logs
- API quota usage tracking
- Error rate monitoring
- Performance metrics collection

## Troubleshooting

### Common Issues

**Authentication Failures:**
```
Error: Invalid access_token provided
Solution: Verify token from HubSpot Private Apps page, ensure proper scopes
```

**Connection Timeouts:**  
```
Error: Connection to HubSpot failed: timeout
Solution: Check network connectivity, verify API endpoint accessibility
```

**Permission Errors:**
```
Error: Insufficient permissions for contacts
Solution: Update app scopes in HubSpot, regenerate access token
```

**Rate Limiting:**
```
Error: API rate limit exceeded  
Solution: Implement delays between requests, use batch operations
```

### Debugging Steps

1. **Verify Credentials**: Test connection in HubSpot API explorer
2. **Check Scopes**: Ensure app has required permissions (contacts, companies, deals)
3. **Validate Network**: Test connectivity to api.hubapi.com
4. **Review Logs**: Check MindsDB logs for detailed error messages
5. **Test Queries**: Start with simple SELECT queries before complex operations

### Performance Tuning

**Query Optimization:**
- Use LIMIT clauses for large datasets
- Implement WHERE filtering to reduce data transfer
- Use specific column selection instead of SELECT *

**Batch Operations:**
- Group multiple INSERT/UPDATE operations
- Use transaction-like patterns for data consistency
- Monitor API quota usage during bulk operations

## Known Limitations

**API Limitations:**
- Rate limits: 100 requests per 10 seconds (varies by HubSpot plan)
- Data retention: Deleted records may still appear for 90 days
- Custom properties: Limited to properties available in your HubSpot account

**Integration Limitations:**  
- Real-time sync: Not available (pull-based only)
- Complex relationships: Cross-object joins require multiple queries
- Large datasets: Pagination required for >10,000 records

**Planned Enhancements:**
- Additional object types (Products, Line Items, Tickets)
- Real-time webhook support
- Advanced analytics and reporting functions
- Custom object support

## API Coverage

| HubSpot API | Coverage | Operations | Notes |
|-------------|----------|------------|--------|
| Companies API | ✅ Complete | GET, POST, PATCH, DELETE | Full CRUD operations |
| Contacts API | ✅ Complete | GET, POST, PATCH, DELETE | Email validation included |
| Deals API | ✅ Complete | GET, POST, PATCH, DELETE | Pipeline management |
| Properties API | 🔄 Partial | GET | Used for metadata discovery |
| Search API | 📋 Planned | GET | Advanced filtering planned |
| Webhooks | 📋 Planned | Subscription | Real-time updates planned |

**Legend:**
- ✅ Complete: Fully implemented and tested
- 🔄 Partial: Basic functionality implemented  
- 📋 Planned: Scheduled for future releases
