from abc import ABC, abstractmethod
from typing import Dict, Any
from pathlib import Path


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
        if not kwargs.get("private_key_path"):
            raise ValueError("private_key_path must be provided when auth_type is 'key_pair'.")

        if not all(kwargs.get(key) for key in ["account", "user", "database"]):
            raise ValueError("Required parameters (account, user, database) must be provided.")

        private_key_path = kwargs.get("private_key_path")
        if not Path(private_key_path).exists():
            raise ValueError(f"Private key file not found: {private_key_path}")

        config = {
            "account": kwargs.get("account"),
            "user": kwargs.get("user"),
            "private_key_file": private_key_path,
            "database": kwargs.get("database"),
            "schema": kwargs.get("schema"),
            "role": kwargs.get("role"),
            "warehouse": kwargs.get("warehouse"),
            "authenticator": "SNOWFLAKE_JWT",
            "auth_type": "key_pair",
        }
        if kwargs.get("private_key_passphrase"):
            config["private_key_file_pwd"] = kwargs.get("private_key_passphrase")
        return config
