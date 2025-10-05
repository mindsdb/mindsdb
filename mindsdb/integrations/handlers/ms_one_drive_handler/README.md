---
title: Microsoft One Drive
sidebarTitle: Microsoft One Drive
---

This documentation describes the integration of MindsDB with [Microsoft OneDrive](https://www.microsoft.com/en-us/microsoft-365/onedrive/online-cloud-storage), a cloud storage service that lets you back up, access, edit, share, and sync your files from any device.

## Prerequisites

1. Before proceeding, ensure that MindsDB is installed locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. Register an application in the [Azure portal](https://portal.azure.com/).
    - Navigate to the [Azure Portal](https://portal.azure.com/#home) and sign in with your Microsoft account.
    - Locate the **Microsoft Entra ID** service and click on it.
    - Click on **App registrations** and then click on **New registration**.
    - Enter a name for your application and select the `Accounts in this organizational directory only` option for the **Supported account types** field.
    - Keep the **Redirect URI** field empty and click on **Register**.
    - Click on **API permissions** and then click on **Add a permission**.
    - Select **Microsoft Graph** and then click on **Delegated permissions**.
    - Search for the `Files.Read` permission and select it.
    - Click on **Add permissions**.
    - Request an administrator to grant consent for the above permissions. If you are the administrator, click on **Grant admin consent for [your organization]** and then click on **Yes**.
    - Copy the **Application (client) ID** and record it as the `client_id` parameter, and copy the **Directory (tenant) ID** and record it as the `tenant_id` parameter.
    - Click on **Certificates & secrets** and then click on **New client secret**.
    - Enter a description for your client secret and select an expiration period.
    - Click on **Add** and copy the generated client secret and record it as the `client_secret` parameter.
    - Click on **Authentication** and then click on **Add a platform**.
    - Select **Web** and enter URL where MindsDB has been deployed followed by `/verify-auth` in the **Redirect URIs** field. For example, if you are running MindsDB locally (on `http://localhost:47334`), enter `http://localhost:47334/verify-auth` in the **Redirect URIs** field.

## Connection

### Modern Token-Based Authentication (Recommended)

For production multi-tenant scenarios, use token injection where your backend manages OAuth flow and provides tokens:

```sql
CREATE DATABASE one_drive_datasource
WITH
    engine = 'one_drive',
    parameters = {
        "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
        "refresh_token": "0.AXoA1234567890...",
        "expires_at": 1735689600,
        "tenant_id": "abcdef12-3456-7890-abcd-ef1234567890",
        "account_id": "user@example.com",
        "client_id": "12345678-90ab-cdef-1234-567890abcdef",
        "client_secret": "abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx"
    };
```

**Token-based parameters:**
* `access_token`: Current access token for Microsoft Graph API (required)
* `refresh_token`: Refresh token for automatic token renewal (recommended)
* `expires_at`: Unix timestamp when access_token expires (optional, defaults to 1 hour)
* `tenant_id`: Azure AD tenant ID (required)
* `account_id`: User account identifier (optional, for tracking)
* `client_id`: Application client ID (required for refresh)
* `client_secret`: Application client secret (required for refresh)
* `authority`: Azure AD authority (optional, defaults to 'common')
* `scopes`: Comma-separated list of scopes (optional, defaults to '.default')

### Legacy Code-Based Authentication

For simple scenarios and backwards compatibility:

```sql
CREATE DATABASE one_drive_datasource
WITH
    engine = 'one_drive',
    parameters = {
        "client_id": "12345678-90ab-cdef-1234-567890abcdef",
        "client_secret": "abcd1234efgh5678ijkl9012mnop3456qrst7890uvwx",
        "tenant_id": "abcdef12-3456-7890-abcd-ef1234567890"
    };
```

**Legacy parameters:**
* `client_id`: The client ID of the registered application
* `client_secret`: The client secret of the registered application
* `tenant_id`: The tenant ID of the registered application
* `code`: Authorization code (automatically handled via redirect flow)

<Note>
The handler automatically detects which authentication method to use based on provided parameters. Token-based auth provides better security, automatic refresh, and per-connection isolation.
</Note>

### Updating Tokens (Token Rotation)

Use `ALTER DATABASE` to rotate tokens without recreating the connection:

```sql
ALTER DATABASE one_drive_datasource
SET PARAMETERS = {
    "access_token": "new_access_token_here",
    "refresh_token": "new_refresh_token_here",
    "expires_at": 1735693200
};
```

## Usage

Retrieve data from a specified file in Microsoft OneDrive by providing the integration name and the file name:

```sql
SELECT *
FROM one_drive_datasource.`my-file.csv`;
LIMIT 10;
```

<Tip>
Wrap the object key in backticks (\`) to avoid any issues parsing the SQL statements provided. This is especially important when the file name contains spaces, special characters or prefixes, such as `my-folder/my-file.csv`.

At the moment, the supported file formats are CSV, TSV, JSON, and Parquet. 
</Tip>

<Note>
The above examples utilize `one_drive_datasource` as the datasource name, which is defined in the `CREATE DATABASE` command.
</Note>

### System Tables

The handler exposes several system tables for metadata and operations:

#### Files Table

List all files available in Microsoft OneDrive:

```sql
SELECT *
FROM one_drive_datasource.files LIMIT 10
```

Retrieve file content explicitly:

```sql
SELECT path, content
FROM one_drive_datasource.files
WHERE path LIKE '%.csv'
LIMIT 10
```

<Tip>
This table returns all objects regardless of format, but only supported formats (CSV, TSV, JSON, Parquet, PDF, TXT) can be queried directly.
</Tip>

#### Connection Metadata Table

View connection identity and metadata:

```sql
SELECT
    connection_id,
    tenant_id,
    account_id,
    display_name,
    email,
    drive_id,
    created_at,
    updated_at
FROM one_drive_datasource.onedrive_connections;
```

This table stores per-connection identity information fetched from Microsoft Graph API, including user details and drive information.

#### Delta State Table

Track incremental sync state for delta queries:

```sql
SELECT
    scope,
    delta_link,
    cursor_updated_at,
    last_success_at,
    items_synced
FROM one_drive_datasource.onedrive_delta_state;
```

This table maintains delta links for continuing incremental syncs across different scopes (root or specific folders).

#### Sync Statistics Table

View operational metrics and diagnostics:

```sql
SELECT
    sync_id,
    started_at,
    completed_at,
    status,
    items_processed,
    items_added,
    items_modified,
    items_deleted,
    errors_count,
    throttled_count,
    duration_seconds
FROM one_drive_datasource.onedrive_sync_stats
ORDER BY started_at DESC
LIMIT 10;
```

This table records sync run statistics for monitoring and troubleshooting.

## Advanced Features

### Automatic Token Refresh

The handler automatically refreshes access tokens when they expire (or within 5 minutes of expiry) using the provided refresh token. No manual intervention required.

### Per-Connection Isolation

Each connection maintains isolated token storage, preventing cross-tenant data leakage. Storage is keyed by a unique connection identifier derived from tenant, account, and client IDs.

### Delta Queries (Incremental Sync)

Use the client's delta query capabilities for efficient incremental syncs:

```python
# Example: Using the handler programmatically for delta sync
from mindsdb.integrations.handlers.ms_one_drive_handler import MSOneDriveHandler

handler = MSOneDriveHandler('my_onedrive', connection_data)
client = handler.connect()

# Get delta changes from last sync
delta_table = DeltaStateTable(handler)
last_delta_link = delta_table.get_delta_link(scope='root')

delta_result = client.get_delta_items(delta_link=last_delta_link)

# Process changes
for item in delta_result['items']:
    if 'deleted' in item:
        # Handle deletion
        pass
    elif 'file' in item:
        # Handle file change
        pass

# Save new delta link
delta_table.update_delta_link(
    scope='root',
    delta_link=delta_result['delta_link'],
    items_synced=len(delta_result['items'])
)
```

### Scope Validation

The handler validates that required scopes (`Files.Read`, `offline_access`) are granted. Use `enable_files_read_all` parameter to request broader permissions:

```sql
CREATE DATABASE one_drive_datasource
WITH engine = 'one_drive',
parameters = {
    ...
    "enable_files_read_all": true
};
```

## Migration Guide

### Migrating from Legacy to Token-Based Auth

If you're currently using the legacy code-based authentication:

**Step 1:** Implement OAuth flow in your backend to obtain tokens

```python
# Example backend OAuth flow
import msal

app = msal.ConfidentialClientApplication(
    client_id="your_client_id",
    client_credential="your_client_secret",
    authority="https://login.microsoftonline.com/your_tenant_id"
)

# After user authorization
result = app.acquire_token_by_authorization_code(
    code=auth_code,
    scopes=["https://graph.microsoft.com/Files.Read", "offline_access"],
    redirect_uri="your_redirect_uri"
)

access_token = result['access_token']
refresh_token = result['refresh_token']
expires_in = result['expires_in']
```

**Step 2:** Update MindsDB connection with tokens

```sql
-- Drop old connection
DROP DATABASE one_drive_datasource;

-- Create new connection with tokens
CREATE DATABASE one_drive_datasource
WITH engine = 'one_drive',
parameters = {
    "access_token": "obtained_access_token",
    "refresh_token": "obtained_refresh_token",
    "expires_at": <current_timestamp + expires_in>,
    "tenant_id": "your_tenant_id",
    "account_id": "user@example.com",
    "client_id": "your_client_id",
    "client_secret": "your_client_secret"
};
```

**Step 3:** Verify migration

```sql
-- Check connection identity
SELECT * FROM one_drive_datasource.onedrive_connections;

-- Test file access
SELECT * FROM one_drive_datasource.files LIMIT 5;
```

### Token Cache Migration

The new handler uses per-connection token caches instead of the global `cache.bin`. On first use with tokens:

1. The handler will create a new cache file: `token_cache_{connection_id}.bin`
2. Legacy `cache.bin` is no longer used and can be safely deleted
3. Token metadata is stored separately in `token_metadata_{connection_id}.json`

### Breaking Changes

- **Global cache removed:** Token caches are now per-connection
- **New system tables:** `onedrive_connections`, `onedrive_delta_state`, `onedrive_sync_stats`
- **Automatic token refresh:** Requires `refresh_token` and valid `client_secret`
- **Scope validation:** Handler validates minimum required scopes on connect

## Troubleshooting Guide

<Warning>
**Database Connection Error**

* **Symptoms**: Failure to connect MindsDB with Microsoft OneDrive.
* **Checklist**:
    1. **Token-based auth:** Ensure `access_token` is valid and not expired. Check `expires_at` timestamp.
    2. **Legacy auth:** Ensure `client_id`, `client_secret` and `tenant_id` are correctly provided.
    3. Verify the registered application has required permissions (`Files.Read`, `offline_access`).
    4. Check that the client secret is not expired in Azure Portal.
    5. Verify tenant ID matches the registered application's directory.
</Warning>

<Warning>
**Token Refresh Failed**

* **Symptoms**: Connection fails after initial success, logs show "Token refresh failed".
* **Checklist**:
    1. Ensure `refresh_token` is provided in connection parameters.
    2. Verify `client_id` and `client_secret` are correct and not expired.
    3. Check that `offline_access` scope was granted during initial authorization.
    4. Review handler logs for specific error messages from MSAL.
    5. If persistent, re-authorize the application and update tokens via `ALTER DATABASE`.
</Warning>

<Warning>
**Missing Required Scopes**

* **Symptoms**: Error message about missing scopes on connection.
* **Checklist**:
    1. Ensure application registration includes at minimum `Files.Read` and `offline_access` permissions.
    2. Admin consent must be granted for the scopes in Azure Portal.
    3. When requesting tokens, include correct scopes in authorization request.
    4. For broader access, set `enable_files_read_all: true` and grant `Files.Read.All` permission.
</Warning>

<Warning>
**SQL Statement Cannot Be Parsed**

* **Symptoms**: SQL queries failing or not recognizing file names with spaces/special characters.
* **Checklist**:
    1. Enclose file names with spaces or special characters in backticks.
    2. Examples:
        * ❌ Incorrect: `SELECT * FROM integration.travel/travel_data.csv`
        * ❌ Incorrect: `SELECT * FROM integration.'travel/travel_data.csv'`
        * ✅ Correct: ``SELECT * FROM integration.`travel/travel_data.csv` ``
</Warning>

<Warning>
**Throttling / Rate Limits**

* **Symptoms**: Queries slow down or fail with 429 errors.
* **Checklist**:
    1. The handler automatically respects `Retry-After` headers from Microsoft Graph API.
    2. Check `onedrive_sync_stats` table for `throttled_count` to monitor throttling events.
    3. Consider implementing request batching or reducing query frequency.
    4. Review Microsoft Graph API throttling guidance for your subscription tier.
</Warning>

<Warning>
**Per-Connection Isolation Issues**

* **Symptoms**: Tokens mixing between connections or cross-tenant data leakage concerns.
* **Checklist**:
    1. Each connection generates a unique `connection_id` based on tenant/account/client IDs.
    2. Token caches are stored as `token_cache_{connection_id}.bin` - isolated per connection.
    3. Verify different connections have different `tenant_id` or `account_id` values.
    4. Check `onedrive_connections` table to see stored connection metadata.
</Warning>