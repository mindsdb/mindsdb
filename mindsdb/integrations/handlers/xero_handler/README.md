# Xero Handler for MindsDB

This handler provides read-only access to Xero Accounting API data through MindsDB. It supports OAuth2 authentication with automatic token refresh and exposes 10 key accounting endpoints as queryable tables.

## Installation

The Xero handler is automatically installed with MindsDB. If you need to install it manually:

```bash
pip install xero-python requests
```

## Supported Tables

The handler provides access to the following Xero Accounting API endpoints:

1. **budgets** - Budget information
2. **contacts** - Customer and supplier contacts
3. **invoices** - Invoice records
4. **items** - Products and services
5. **overpayments** - Overpayment records
6. **payments** - Payment records
7. **purchase_orders** - Purchase order records
8. **quotes** - Sales quote records
9. **repeating_invoices** - Repeating invoice templates
10. **accounts** - Chart of accounts

All tables support SELECT queries with WHERE, ORDER BY, and LIMIT clauses.

## Connection Setup

### 1. Create a Xero App

1. Go to [Xero Developer Portal](https://developer.xero.com)
2. Sign in with your Xero account
3. Click "My Apps" and create a new app
4. Choose "Web" as the app type
5. Fill in the app details:
   - App Name
   - Company URL
   - Redirect URI (e.g., `http://localhost:3000/callback`)
6. Note your Client ID and Client Secret

### 2. Configure the Connection in MindsDB

#### Initial Connection (First Time)

```sql
CREATE DATABASE xero_connection
WITH ENGINE = 'xero',
PARAMETERS = {
    'client_id': 'your_client_id',
    'client_secret': 'your_client_secret',
    'redirect_uri': 'http://localhost:3000/callback'
};
```

When you run this for the first time, MindsDB will return an authorization URL. Visit this URL and authorize the application. You'll receive an authorization code.

#### With Authorization Code

After authorizing, provide the authorization code to complete the setup:

```sql
CREATE DATABASE xero_connection
WITH ENGINE = 'xero',
PARAMETERS = {
    'client_id': 'your_client_id',
    'client_secret': 'your_client_secret',
    'redirect_uri': 'http://localhost:3000/callback',
    'code': 'your_authorization_code'
};
```

#### Specifying Tenant (Organization)

If you have access to multiple Xero organizations, you can specify which one to use:

```sql
CREATE DATABASE xero_connection
WITH ENGINE = 'xero',
PARAMETERS = {
    'client_id': 'your_client_id',
    'client_secret': 'your_client_secret',
    'redirect_uri': 'http://localhost:3000/callback',
    'code': 'your_authorization_code',
    'tenant_id': 'your_tenant_id'
};
```

If not specified, the handler will use the first accessible organization.

## Usage Examples

### Select All Contacts

```sql
SELECT * FROM xero_connection.contacts LIMIT 10;
```

### Find Recent Invoices

```sql
SELECT invoice_number, contact_name, total, invoice_date
FROM xero_connection.invoices
WHERE status = 'DRAFT'
ORDER BY invoice_date DESC
LIMIT 20;
```

### Get All Active Accounts

```sql
SELECT code, name, type, currency_code
FROM xero_connection.accounts
WHERE status = 'ACTIVE'
ORDER BY code;
```

### View Purchase Orders

```sql
SELECT purchase_order_number, contact_name, total, order_date
FROM xero_connection.purchase_orders
WHERE status = 'AUTHORISED'
LIMIT 50;
```

### Check Available Items

```sql
SELECT code, description, is_tracked_as_inventory
FROM xero_connection.items
LIMIT 30;
```

### Get Payments Summary

```sql
SELECT invoice_id, amount, payment_type, reference
FROM xero_connection.payments
WHERE amount > 100
ORDER BY updated_utc DESC;
```

### View Quotes

```sql
SELECT quote_number, contact_name, total, quote_date, expiry_date
FROM xero_connection.quotes
WHERE status = 'DRAFT'
ORDER BY quote_date DESC;
```

## Authentication Details

### OAuth2 Flow

This handler implements the OAuth2 Authorization Code flow as documented in the [Xero OAuth2 Guide](https://developer.xero.com/documentation/guides/oauth2/auth-flow).

1. **Authorization**: User is directed to Xero's authorization endpoint
2. **Code Exchange**: Authorization code is exchanged for access and refresh tokens
3. **Token Storage**: Tokens are stored securely in MindsDB's encrypted storage
4. **Token Refresh**: Access tokens are automatically refreshed when expired (Xero tokens expire every 30 minutes)

### Scopes

The handler requests the following OAuth2 scopes:
- `openid` - OpenID Connect
- `profile` - User profile information
- `email` - User email
- `accounting.transactions` - Read access to transactions
- `accounting.settings` - Read access to settings
- `offline_access` - Refresh token access

## Limitations

- **Read-only**: This handler provides read-only access to the Xero API. Create, update, and delete operations are not supported.
- **Rate Limiting**: Xero API has rate limits. Be mindful of the number of queries you execute.
- **Token Expiration**: Access tokens expire after 30 minutes. The handler automatically refreshes them as needed.
- **Tenant Selection**: If you have access to multiple Xero organizations, you must specify the `tenant_id` or the handler will use the first available organization.

## Troubleshooting

### "Authorization required" Error

If you see this error during connection, you need to provide an authorization code:
1. Visit the URL provided in the error message
2. Authorize the application
3. Copy the authorization code
4. Update your connection parameters with the code

### "No Xero tenants found" Error

This typically means:
1. The user account has no Xero organizations/tenants
2. The user needs to create a Xero organization first
3. The user account permissions are restricted

### Token Refresh Failures

If token refresh fails:
1. Delete the current connection and create a new one
2. Go through the authorization flow again
3. This will generate new tokens

## Advanced Usage

### Query Filtering

You can use WHERE clauses to filter data:

```sql
SELECT * FROM xero_connection.invoices
WHERE invoice_date >= '2024-01-01'
AND status = 'DRAFT'
AND total > 1000;
```

### Ordering and Limiting

```sql
SELECT contact_name, total
FROM xero_connection.payments
ORDER BY total DESC
LIMIT 5;
```

### Column Selection

Only select the columns you need for better performance:

```sql
SELECT invoice_number, total, due_date
FROM xero_connection.invoices;
```

## API Reference

For detailed information about the Xero Accounting API, visit:
- [Xero API Documentation](https://developer.xero.com/documentation/api/accounting/overview)
- [Xero Python SDK](https://github.com/XeroAPI/xero-python)

## Support

For issues or questions:
1. Check the [Xero Developer Community](https://community.xero.com/developer)
2. Review the [xero-python SDK documentation](https://github.com/XeroAPI/xero-python)
3. Check MindsDB documentation for handler-specific issues
