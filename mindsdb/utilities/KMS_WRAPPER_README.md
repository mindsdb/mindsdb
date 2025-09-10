# KMS Encryption for Integration Data

This document describes the new KMS encryption feature for the `Integration.data` column in MindsDB.

## Overview

The `Integration` table's `data` column now supports optional KMS (Key Management Service) encryption. When enabled, sensitive integration data such as database passwords, API keys, and connection strings are encrypted before being stored in the database.

## Features

- **Multiple KMS Providers**: Supports AWS KMS/Secrets Manager, Azure Key Vault, Google Cloud Secret Manager, and local encryption
- **Optional Encryption**: Encryption is controlled by configuration - can be enabled or disabled
- **Transparent Operation**: Applications using the Integration model don't need to change - encryption/decryption happens automatically
- **Backward Compatibility**: Existing data continues to work when encryption is disabled
- **Environment Variable Support**: Easy configuration via environment variables
- **Automatic Fallback**: Falls back to local encryption if KMS provider is unavailable
- **Migration Support**: Includes migration scripts for existing data

## Configuration

### KMS Providers

The system supports four KMS providers:

1. **Local**: Uses MindsDB's built-in encryption (default)
2. **AWS**: Uses AWS KMS or Secrets Manager
3. **Azure**: Uses Azure Key Vault
4. **GCP**: Uses Google Cloud Secret Manager

### Environment Variables

#### Basic Configuration
```bash
# Enable KMS encryption
export MINDSDB_KMS_ENABLED=1

# Set the provider (local, aws, azure, gcp)
export MINDSDB_KMS_PROVIDER=aws

# Set the secret key for local encryption
export MINDSDB_KMS_SECRET_KEY=your-secret-key-here
```

#### AWS Configuration
```bash
# AWS KMS/Secrets Manager
export MINDSDB_KMS_PROVIDER=aws
export MINDSDB_KMS_AWS_REGION=us-east-1
export MINDSDB_KMS_AWS_KEY_ID=arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
# OR use Secrets Manager
export MINDSDB_KMS_AWS_SECRET_NAME=mindsdb-encryption-key
# Optional: AWS credentials (if not using IAM roles)
export MINDSDB_KMS_AWS_ACCESS_KEY_ID=your-access-key
export MINDSDB_KMS_AWS_SECRET_ACCESS_KEY=your-secret-key
```

#### Azure Configuration
```bash
# Azure Key Vault
export MINDSDB_KMS_PROVIDER=azure
export MINDSDB_KMS_AZURE_VAULT_URL=https://your-vault.vault.azure.net/
export MINDSDB_KMS_AZURE_SECRET_NAME=mindsdb-encryption-key
# Optional: Service Principal credentials
export MINDSDB_KMS_AZURE_TENANT_ID=your-tenant-id
export MINDSDB_KMS_AZURE_CLIENT_ID=your-client-id
export MINDSDB_KMS_AZURE_CLIENT_SECRET=your-client-secret
```

#### GCP Configuration
```bash
# Google Cloud Secret Manager
export MINDSDB_KMS_PROVIDER=gcp
export MINDSDB_KMS_GCP_PROJECT_ID=your-project-id
export MINDSDB_KMS_GCP_SECRET_ID=mindsdb-encryption-key
export MINDSDB_KMS_GCP_VERSION_ID=latest
```

### Configuration File

Add to your `config.json`:

```json
{
  "kms": {
    "enabled": true,
    "provider": "aws",
    "secret_key": "fallback-secret-key",
    "aws": {
      "region": "us-east-1",
      "key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
      "secret_name": "mindsdb-encryption-key",
      "access_key_id": "your-access-key",
      "secret_access_key": "your-secret-key"
    },
    "azure": {
      "vault_url": "https://your-vault.vault.azure.net/",
      "secret_name": "mindsdb-encryption-key",
      "tenant_id": "your-tenant-id",
      "client_id": "your-client-id",
      "client_secret": "your-client-secret"
    },
    "gcp": {
      "project_id": "your-project-id",
      "secret_id": "mindsdb-encryption-key",
      "version_id": "latest"
    }
  }
}
```

### Programmatic Configuration

```python
from mindsdb.utilities.config import config

# AWS KMS
config['kms'] = {
    'enabled': True,
    'provider': 'aws',
    'aws': {
        'region': 'us-east-1',
        'key_id': 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012'
    }
}

# Azure Key Vault
config['kms'] = {
    'enabled': True,
    'provider': 'azure',
    'azure': {
        'vault_url': 'https://your-vault.vault.azure.net/',
        'secret_name': 'mindsdb-encryption-key'
    }
}

# GCP Secret Manager
config['kms'] = {
    'enabled': True,
    'provider': 'gcp',
    'gcp': {
        'project_id': 'your-project-id',
        'secret_id': 'mindsdb-encryption-key'
    }
}
```

## Usage

### Creating an Integration with Encrypted Data

```python
from mindsdb.interfaces.storage.db import Integration, session

# Create integration with sensitive data
integration_data = {
    "host": "db.example.com",
    "port": 5432,
    "username": "db_user",
    "password": "secret_password",
    "database": "production_db"
}

integration = Integration(
    name="my_postgres",
    engine="postgres",
    data=integration_data,
    company_id=0
)

session.add(integration)
session.commit()
```

### Retrieving Integration Data

```python
# Retrieve integration - data is automatically decrypted if encryption is enabled
integration = session.query(Integration).filter_by(name="my_postgres").first()
print(integration.data)  # Returns decrypted data
```

## Migration

### Migrating Existing Data

To migrate existing integrations to support encryption:

```bash
python -m mindsdb.migrations.encrypted_integration_data
```

### Rolling Back Migration

To rollback the migration (convert encrypted data back to regular JSON):

```bash
python -m mindsdb.migrations.encrypted_integration_data --rollback
```

**Note**: Ensure KMS encryption is disabled before running rollback.

## Required Dependencies

### AWS KMS/Secrets Manager
```bash
pip install boto3
```

### Azure Key Vault
```bash
pip install azure-keyvault-secrets azure-identity
```

### Google Cloud Secret Manager
```bash
pip install google-cloud-secret-manager
```

## Implementation Details

### KMS Wrapper Architecture

The implementation uses a unified KMS wrapper (`KMSWrapper`) that:

- Supports multiple KMS providers through a common interface
- Automatically falls back to local encryption if KMS is unavailable
- Handles provider-specific authentication and configuration
- Provides transparent encryption/decryption operations

### Custom SQLAlchemy Type

The `EncryptedJson` type:

- Extends `types.TypeDecorator` with `LargeBinary` as the underlying type
- Uses the KMS wrapper for encryption/decryption operations
- Automatically encrypts data on insert when KMS is enabled
- Automatically decrypts data on select when KMS is enabled
- Falls back to regular JSON storage when KMS is disabled

### Encryption Methods

- **Local**: Uses MindsDB's built-in Fernet encryption with SHA-256 key derivation
- **AWS**: Uses AWS KMS for direct encryption or Secrets Manager for key storage
- **Azure**: Uses Azure Key Vault for key storage with local encryption
- **GCP**: Uses Google Cloud Secret Manager for key storage with local encryption

### Database Schema

The `Integration.data` column now uses the `EncryptedJson` type instead of the regular `Json` type. This change is transparent to applications using the model.

## Security Considerations

1. **Key Management**: Store the secret key securely. Consider using a proper key management service in production.

2. **Key Rotation**: If you need to rotate keys, you'll need to:
   - Decrypt all data with the old key
   - Re-encrypt with the new key
   - Update the configuration

3. **Backup Security**: Encrypted data in backups will be encrypted, but ensure backup access is also secured.

4. **Performance**: Encryption/decryption adds minimal overhead, but consider the impact on high-volume operations.

## Example

See `example_encrypted_integration.py` for a complete working example.

## Troubleshooting

### Common Issues

1. **Migration Fails**: Ensure the database is accessible and you have proper permissions.

2. **Decryption Errors**: Verify that the secret key matches the one used for encryption.

3. **Configuration Not Applied**: Restart MindsDB after changing configuration.

### Debug Mode

Enable debug logging to see encryption/decryption operations:

```python
import logging
logging.getLogger('mindsdb').setLevel(logging.DEBUG)
```

## Future Enhancements

Potential future improvements:

1. **AWS KMS Integration**: Direct integration with AWS KMS for key management
2. **Key Rotation Support**: Built-in support for key rotation
3. **Field-Level Encryption**: Encrypt only specific fields within the data
4. **Audit Logging**: Log encryption/decryption operations for compliance
