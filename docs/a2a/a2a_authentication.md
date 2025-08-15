# Authentication Token Management

This document describes the authentication token management system in MindsDB, which provides token-based authentication for various services including A2A (Agent-to-Agent) servers.

## Overview

The authentication token management system provides token-based authentication for various services (including A2A servers), with the authentication requirement being tied to the HTTP API authentication configuration. This means:

- If HTTP authentication is **enabled** in MindsDB, then token authentication is **required**
- If HTTP authentication is **disabled** in MindsDB, then token authentication is **not required**

## Architecture

The implementation consists of several components:

1. **HTTP API Endpoints** (`/api/auth_tokens/*`) - For token management
2. **Authentication Middleware** - For validating tokens on server requests
3. **Shared Authentication Utilities** - For token storage and validation logic

### Separate Server Architecture

Since services like the A2A server run as separate processes from the main MindsDB HTTP server, token validation is performed via HTTP API calls:

- **HTTP Server**: Stores tokens in memory and provides token management endpoints
- **External Services**: Validate tokens by making HTTP requests to the HTTP server
- **Communication**: External services call `/api/auth_tokens/token/validate` endpoint to verify tokens
- **Configuration**: External services automatically read the HTTP API configuration from the MindsDB config file
- **Agent Card**: A2A server includes authentication information in its agent card when authentication is required

This design allows external services to be completely independent while still maintaining secure token-based authentication, with no additional configuration required.

## HTTP API Endpoints

### Generate Token
```
POST /api/auth_tokens/token
```

Generates a new authentication token. If HTTP authentication is enabled, the user must be authenticated to generate tokens.

**Request Body:**
```json
{
  "description": "Optional description for the token"
}
```

**Response:**
```json
{
  "token": "generated_token_here",
  "expires_in": 86400,
  "description": "Optional description"
}
```

### Validate Token
```
POST /api/auth_tokens/token/validate
```

Validates an existing authentication token.

**Request Body:**
```json
{
  "token": "token_to_validate"
}
```

**Response:**
```json
{
  "valid": true,
  "created_at": 1234567890.123,
  "description": "Token description",
  "user_id": "username"
}
```

### List Tokens
```
GET /api/auth_tokens/tokens
```

Lists all active authentication tokens (admin only). Only available when HTTP authentication is enabled.

**Response:**
```json
{
  "tokens": [
    {
      "token_preview": "abc123...xyz789",
      "created_at": 1234567890.123,
      "description": "Token description",
      "user_id": "username"
    }
  ],
  "count": 1
}
```

### Revoke Token
```
DELETE /api/auth_tokens/token/{token}
```

Revokes an authentication token. Only available when HTTP authentication is enabled.

**Response:**
```json
{
  "message": "Token revoked successfully"
}
```

### Get Status
```
GET /api/auth_tokens/status
```

Returns the current authentication configuration.

**Response:**
```json
{
  "http_auth_enabled": true,
  "a2a_auth_required": true,
  "active_tokens_count": 1
}
```

## Service Authentication

External services (like the A2A server and MCP server) automatically check for authentication based on the HTTP API configuration. Since these services run as separate processes, they validate tokens by making HTTP requests to the main MindsDB HTTP API.

### When HTTP Auth is Disabled
- No authentication required
- All requests to external services are allowed

### When HTTP Auth is Enabled
- Authentication is required
- Requests must include a valid `Authorization: Bearer <token>` header
- Invalid or missing tokens return 401 Unauthorized
- Token validation is performed by calling the HTTP API endpoint `/api/auth_tokens/token/validate`

### MCP Server Authentication

The MCP server supports two authentication modes:

1. **Environment-based MCP token** (priority):
   - Set `MINDSDB_MCP_ACCESS_TOKEN` environment variable
   - Uses the exact token value for authentication
   - Overrides HTTP auth configuration

2. **HTTP auth-based token validation** (fallback):
   - When `MINDSDB_MCP_ACCESS_TOKEN` is not set
   - Uses the same authentication token system as other services
   - Respects HTTP auth configuration (enabled/disabled)

### Configuration

External services automatically read the HTTP API configuration from the MindsDB config file. They use the same `api.http.host` and `api.http.port` settings that the main MindsDB server uses.

No additional configuration is required - external services will automatically connect to the correct HTTP API endpoint.

### Agent Card Authentication Information

The A2A server automatically includes authentication information in its agent card when authentication is required:

**When HTTP Auth is Disabled:**
```json
{
  "name": "MindsDB Agent Connector",
  "version": "1.0.0",
  "capabilities": { ... },
  "skills": [ ... ]
  // No authentication field
}
```

**When HTTP Auth is Enabled:**
```json
{
  "name": "MindsDB Agent Connector",
  "version": "1.0.0",
  "capabilities": { ... },
  "skills": [ ... ],
  "authentication": {
    "schemes": ["bearer"],
    "credentials": "Authentication token required. Generate token via /api/auth_tokens/token endpoint."
  }
}
```

This allows A2A clients to automatically discover authentication requirements and provide appropriate credentials.

## Usage Examples

### 1. Generate a Token (when HTTP auth is disabled)
```bash
curl -X POST http://localhost:47334/api/auth_tokens/token \
  -H "Content-Type: application/json" \
  -d '{"description": "My authentication token"}'
```

### 2. Generate a Token (when HTTP auth is enabled)
```bash
# First, login to get a session
curl -X POST http://localhost:47334/api/login \
  -H "Content-Type: application/json" \
  -d '{"username": "mindsdb", "password": "mindsdb"}' \
  -c cookies.txt

# Then generate authentication token
curl -X POST http://localhost:47334/api/auth_tokens/token \
  -H "Content-Type: application/json" \
  -d '{"description": "My authentication token"}' \
  -b cookies.txt
```

### 3. Start A2A Server
```bash
# Start A2A server (automatically uses HTTP API config)
python -m mindsdb.api.a2a
```

### 4. Use Token with A2A Server
```bash
# Get A2A server status with token
curl -X GET http://localhost:10002/status \
  -H "Authorization: Bearer your_a2a_token_here"
```

### 5. Use Token with MCP Server
```bash
# Connect to MCP server with authentication token
# (when MINDSDB_MCP_ACCESS_TOKEN is not set and HTTP auth is enabled)
curl -X POST http://localhost:47337/ \
  -H "Authorization: Bearer your_authentication_token_here" \
  -H "Content-Type: application/json" \
  -d '{"method": "tools/list", "params": {}}'
```

### 6. Validate a Token
```bash
curl -X POST http://localhost:47334/api/auth_tokens/token/validate \
  -H "Content-Type: application/json" \
  -d '{"token": "your_authentication_token_here"}'
```

## Configuration

The A2A authentication behavior is controlled by the HTTP authentication configuration in MindsDB:

```json
{
  "auth": {
    "http_auth_enabled": true,
    "username": "mindsdb",
    "password": "mindsdb"
  }
}
```

- `http_auth_enabled: true` → A2A authentication required
- `http_auth_enabled: false` → A2A authentication not required

## Token Security

- Tokens are generated using `secrets.token_urlsafe(32)` for high entropy
- Tokens expire after 24 hours
- Tokens are stored in memory (in production, consider using a database)
- Token previews in listings only show first 8 and last 8 characters for security

## Testing

Use the provided test script to verify the implementation:

```bash
python test_a2a_auth.py
```

This script tests:
1. A2A status endpoint
2. Token generation
3. Token validation
4. Token listing
5. A2A server access with valid token
6. A2A server access with invalid token

## Implementation Details

### Files Modified/Created

1. **`mindsdb/utilities/a2a_auth.py`** - Shared authentication utilities
2. **`mindsdb/api/http/namespaces/configs/a2a.py`** - A2A namespace configuration
3. **`mindsdb/api/http/namespaces/a2a.py`** - A2A HTTP API endpoints
4. **`mindsdb/api/a2a/common/server/auth_middleware.py`** - A2A authentication middleware
5. **`mindsdb/api/a2a/common/server/server.py`** - Updated to use auth middleware
6. **`mindsdb/api/http/initialize.py`** - Added A2A namespace to HTTP API

### Key Features

- **Conditional Authentication**: A2A auth requirement tied to HTTP auth configuration
- **Token Management**: Full CRUD operations for A2A tokens
- **Security**: Secure token generation and validation
- **Logging**: Comprehensive logging for debugging and monitoring
- **Error Handling**: Proper HTTP status codes and error messages
