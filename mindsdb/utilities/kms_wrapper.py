"""
KMS (Key Management Service) wrapper supporting multiple cloud providers.

This module provides a unified interface for encrypting/decrypting data using:
- AWS KMS/Secrets Manager
- Azure Key Vault
- Google Cloud Secret Manager
- Local encryption (fallback)
"""

import json
import base64
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

from mindsdb.utilities.functions import encrypt_json, decrypt_json
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class KMSProvider(ABC):
    """Abstract base class for KMS providers."""
    
    @abstractmethod
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt the given data."""
        pass
    
    @abstractmethod
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt the given data."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the KMS provider is available and configured."""
        pass


class LocalKMSProvider(KMSProvider):
    """Local encryption provider using MindsDB's built-in encryption."""
    
    def __init__(self, secret_key: str):
        self.secret_key = secret_key
    
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt data using local encryption."""
        return encrypt_json(data, self.secret_key)
    
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt data using local encryption."""
        return decrypt_json(encrypted_data, self.secret_key)
    
    def is_available(self) -> bool:
        """Local provider is always available."""
        return True


class AWSKMSProvider(KMSProvider):
    """AWS KMS/Secrets Manager provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._client = None
        self._secrets_client = None
        self._kms_client = None
    
    def _get_kms_client(self):
        """Get or create KMS client."""
        if self._kms_client is None:
            try:
                import boto3
                from botocore.exceptions import ClientError, NoCredentialsError
                
                self._kms_client = boto3.client(
                    'kms',
                    region_name=self.config.get('region', 'us-east-1'),
                    aws_access_key_id=self.config.get('access_key_id'),
                    aws_secret_access_key=self.config.get('secret_access_key')
                )
            except ImportError:
                raise ImportError("boto3 is required for AWS KMS support. Install with: pip install boto3")
            except Exception as e:
                logger.error(f"Failed to create AWS KMS client: {e}")
                raise
        
        return self._kms_client
    
    def _get_secrets_client(self):
        """Get or create Secrets Manager client."""
        if self._secrets_client is None:
            try:
                import boto3
                
                self._secrets_client = boto3.client(
                    'secretsmanager',
                    region_name=self.config.get('region', 'us-east-1'),
                    aws_access_key_id=self.config.get('access_key_id'),
                    aws_secret_access_key=self.config.get('secret_access_key')
                )
            except ImportError:
                raise ImportError("boto3 is required for AWS Secrets Manager support. Install with: pip install boto3")
            except Exception as e:
                logger.error(f"Failed to create AWS Secrets Manager client: {e}")
                raise
        
        return self._secrets_client
    
    def _get_encryption_key(self) -> str:
        """Get encryption key from AWS Secrets Manager or use provided key."""
        if 'secret_name' in self.config:
            try:
                secrets_client = self._get_secrets_client()
                response = secrets_client.get_secret_value(SecretId=self.config['secret_name'])
                return response['SecretString']
            except Exception as e:
                logger.error(f"Failed to get secret from AWS Secrets Manager: {e}")
                raise
        
        return self.config.get('secret_key', '')
    
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt data using AWS KMS."""
        try:
            kms_client = self._get_kms_client()
            key_id = self.config.get('key_id')
            
            if key_id:
                # Use AWS KMS for encryption
                json_str = json.dumps(data)
                response = kms_client.encrypt(
                    KeyId=key_id,
                    Plaintext=json_str
                )
                return base64.b64encode(response['CiphertextBlob'])
            else:
                # Fall back to local encryption with key from Secrets Manager
                secret_key = self._get_encryption_key()
                return encrypt_json(data, secret_key)
                
        except Exception as e:
            logger.error(f"AWS KMS encryption failed: {e}")
            raise
    
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt data using AWS KMS."""
        try:
            kms_client = self._get_kms_client()
            key_id = self.config.get('key_id')
            
            if key_id:
                # Use AWS KMS for decryption
                ciphertext_blob = base64.b64decode(encrypted_data)
                response = kms_client.decrypt(CiphertextBlob=ciphertext_blob)
                return json.loads(response['Plaintext'])
            else:
                # Fall back to local decryption with key from Secrets Manager
                secret_key = self._get_encryption_key()
                return decrypt_json(encrypted_data, secret_key)
                
        except Exception as e:
            logger.error(f"AWS KMS decryption failed: {e}")
            raise
    
    def is_available(self) -> bool:
        """Check if AWS KMS is available."""
        try:
            self._get_kms_client()
            return True
        except Exception:
            return False


class AzureKMSProvider(KMSProvider):
    """Azure Key Vault provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._client = None
    
    def _get_client(self):
        """Get or create Azure Key Vault client."""
        if self._client is None:
            try:
                from azure.keyvault.secrets import SecretClient
                from azure.identity import DefaultAzureCredential, ClientSecretCredential
                
                vault_url = self.config.get('vault_url')
                if not vault_url:
                    raise ValueError("vault_url is required for Azure Key Vault")
                
                # Use client credentials if provided, otherwise use default credential
                if all(k in self.config for k in ['client_id', 'client_secret', 'tenant_id']):
                    credential = ClientSecretCredential(
                        tenant_id=self.config['tenant_id'],
                        client_id=self.config['client_id'],
                        client_secret=self.config['client_secret']
                    )
                else:
                    credential = DefaultAzureCredential()
                
                self._client = SecretClient(vault_url=vault_url, credential=credential)
                
            except ImportError:
                raise ImportError("azure-keyvault-secrets and azure-identity are required for Azure Key Vault support. Install with: pip install azure-keyvault-secrets azure-identity")
            except Exception as e:
                logger.error(f"Failed to create Azure Key Vault client: {e}")
                raise
        
        return self._client
    
    def _get_encryption_key(self) -> str:
        """Get encryption key from Azure Key Vault."""
        try:
            client = self._get_client()
            secret_name = self.config.get('secret_name', 'mindsdb-encryption-key')
            secret = client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Failed to get secret from Azure Key Vault: {e}")
            raise
    
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt data using local encryption with key from Azure Key Vault."""
        try:
            secret_key = self._get_encryption_key()
            return encrypt_json(data, secret_key)
        except Exception as e:
            logger.error(f"Azure Key Vault encryption failed: {e}")
            raise
    
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt data using local decryption with key from Azure Key Vault."""
        try:
            secret_key = self._get_encryption_key()
            return decrypt_json(encrypted_data, secret_key)
        except Exception as e:
            logger.error(f"Azure Key Vault decryption failed: {e}")
            raise
    
    def is_available(self) -> bool:
        """Check if Azure Key Vault is available."""
        try:
            self._get_client()
            return True
        except Exception:
            return False


class GCPKMSProvider(KMSProvider):
    """Google Cloud Secret Manager provider."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._client = None
    
    def _get_client(self):
        """Get or create GCP Secret Manager client."""
        if self._client is None:
            try:
                from google.cloud import secretmanager
                from google.auth.exceptions import DefaultCredentialsError
                
                project_id = self.config.get('project_id')
                if not project_id:
                    raise ValueError("project_id is required for GCP Secret Manager")
                
                self._client = secretmanager.SecretManagerServiceClient()
                
            except ImportError:
                raise ImportError("google-cloud-secret-manager is required for GCP Secret Manager support. Install with: pip install google-cloud-secret-manager")
            except Exception as e:
                logger.error(f"Failed to create GCP Secret Manager client: {e}")
                raise
        
        return self._client
    
    def _get_encryption_key(self) -> str:
        """Get encryption key from GCP Secret Manager."""
        try:
            client = self._get_client()
            project_id = self.config.get('project_id')
            secret_id = self.config.get('secret_id', 'mindsdb-encryption-key')
            version_id = self.config.get('version_id', 'latest')
            
            name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
            
        except Exception as e:
            logger.error(f"Failed to get secret from GCP Secret Manager: {e}")
            raise
    
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt data using local encryption with key from GCP Secret Manager."""
        try:
            secret_key = self._get_encryption_key()
            return encrypt_json(data, secret_key)
        except Exception as e:
            logger.error(f"GCP Secret Manager encryption failed: {e}")
            raise
    
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt data using local decryption with key from GCP Secret Manager."""
        try:
            secret_key = self._get_encryption_key()
            return decrypt_json(encrypted_data, secret_key)
        except Exception as e:
            logger.error(f"GCP Secret Manager decryption failed: {e}")
            raise
    
    def is_available(self) -> bool:
        """Check if GCP Secret Manager is available."""
        try:
            self._get_client()
            return True
        except Exception:
            return False


class KMSWrapper:
    """Unified KMS wrapper supporting multiple providers."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._provider = None
        self._initialize_provider()
    
    def _initialize_provider(self):
        """Initialize the appropriate KMS provider based on configuration."""
        provider_type = self.config.get('provider', 'local').lower()
        
        try:
            if provider_type == 'aws':
                self._provider = AWSKMSProvider(self.config.get('aws', {}))
            elif provider_type == 'azure':
                self._provider = AzureKMSProvider(self.config.get('azure', {}))
            elif provider_type == 'gcp':
                self._provider = GCPKMSProvider(self.config.get('gcp', {}))
            elif provider_type == 'local':
                secret_key = self.config.get('secret_key', 'dummy-key')
                self._provider = LocalKMSProvider(secret_key)
            else:
                raise ValueError(f"Unsupported KMS provider: {provider_type}")
            
            # Check if provider is available
            if not self._provider.is_available():
                logger.warning(f"KMS provider {provider_type} is not available, falling back to local encryption")
                self._provider = LocalKMSProvider(self.config.get('secret_key', 'dummy-key'))
                
        except Exception as e:
            logger.error(f"Failed to initialize KMS provider {provider_type}: {e}")
            logger.info("Falling back to local encryption")
            self._provider = LocalKMSProvider(self.config.get('secret_key', 'dummy-key'))
    
    def encrypt(self, data: Dict[str, Any]) -> bytes:
        """Encrypt data using the configured KMS provider."""
        return self._provider.encrypt(data)
    
    def decrypt(self, encrypted_data: bytes) -> Dict[str, Any]:
        """Decrypt data using the configured KMS provider."""
        return self._provider.decrypt(encrypted_data)
    
    def is_available(self) -> bool:
        """Check if the KMS provider is available."""
        return self._provider.is_available()
    
    def get_provider_type(self) -> str:
        """Get the current provider type."""
        return type(self._provider).__name__.replace('KMSProvider', '').lower()


def create_kms_wrapper(config: Dict[str, Any]) -> KMSWrapper:
    """Factory function to create a KMS wrapper."""
    return KMSWrapper(config)
