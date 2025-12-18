from abc import ABC, abstractmethod
from typing import Dict, Any, Union
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class SnowflakeAuthType(ABC):
    @abstractmethod
    def get_config(self, **kwargs) -> Dict[str, Any]:
        pass


class PasswordAuthType(SnowflakeAuthType):
    def get_config(self, **kwargs) -> Dict[str, Any]:
        required_keys = ["account", "user", "database"]
        if not all(kwargs.get(key) for key in required_keys):
            raise ValueError("Required parameters (account, user, database) must be provided.")

        if not kwargs.get("password"):
            raise ValueError("Password must be provided when auth_type is 'password'.")
        return {
            "account": kwargs.get("account"),
            "user": kwargs.get("user"),
            "password": kwargs.get("password"),
            "database": kwargs.get("database"),
            "schema": kwargs.get("schema"),
            "role": kwargs.get("role"),
            "warehouse": kwargs.get("warehouse"),
            "auth_type": "password",
        }


class KeyPairAuthType(SnowflakeAuthType):
    def get_config(self, **kwargs) -> Dict[str, Any]:
        if not all(kwargs.get(key) for key in ["account", "user", "database"]):
            raise ValueError("Required parameters (account, user, database) must be provided.")

        private_key_value = kwargs.get("private_key")
        private_key_path = kwargs.get("private_key_path")

        if not private_key_value and not private_key_path:
            raise ValueError("Either private_key or private_key_path must be provided when auth_type is 'key_pair'.")

        config = {
            "account": kwargs.get("account"),
            "user": kwargs.get("user"),
            "database": kwargs.get("database"),
            "schema": kwargs.get("schema"),
            "role": kwargs.get("role"),
            "warehouse": kwargs.get("warehouse"),
            "authenticator": "SNOWFLAKE_JWT",
            "auth_type": "key_pair",
        }

        if private_key_value:
            config["private_key"] = self._load_private_key(private_key_value, kwargs.get("private_key_passphrase"))
        else:
            if not Path(private_key_path).exists():
                raise ValueError(f"Private key file not found: {private_key_path}")
            config["private_key_file"] = private_key_path
            if kwargs.get("private_key_passphrase"):
                config["private_key_file_pwd"] = kwargs.get("private_key_passphrase")
        return config

    def _load_private_key(self, private_key: Union[str, bytes], passphrase: str = None):
        if isinstance(private_key, str):
            private_key = private_key.replace("\\n", "\n").encode()
        elif isinstance(private_key, bytes) is False:
            raise ValueError("private_key must be a string or bytes.")

        password = passphrase.encode() if passphrase else None
        try:
            return serialization.load_pem_private_key(private_key, password=password, backend=default_backend())
        except Exception as exc:
            raise ValueError(
                "Failed to load private_key. Ensure it is a valid PEM-encoded key and the passphrase is correct."
            ) from exc
