from abc import ABC, abstractmethod
from typing import Dict, Any


class SnowflakeAuthType(ABC):
    @abstractmethod
    def get_config(self, **kwargs) -> Dict[str, Any]:
        pass


class PasswordAuthType(SnowflakeAuthType):
    def get_config(self, **kwargs) -> Dict[str, Any]:
        return {
            "account": kwargs.get("account"),
            "user": kwargs.get("user"),
            "password": kwargs.get("password"),
            "database": kwargs.get("database"),
            "schema": kwargs.get("schema"),
            "role": kwargs.get("role"),
            "warehouse": kwargs.get("warehouse"),
        }


class KeyPairAuthType(SnowflakeAuthType):
    def get_config(self, **kwargs) -> Dict[str, Any]:
        config = {
            "account": kwargs.get("account"),
            "user": kwargs.get("user"),
            "private_key_file": kwargs.get("private_key_path"),
            "database": kwargs.get("database"),
            "schema": kwargs.get("schema"),
            "role": kwargs.get("role"),
            "warehouse": kwargs.get("warehouse"),
            "authenticator": "SNOWFLAKE_JWT",
        }
        if kwargs.get("private_key_passphrase"):
            config["private_key_file_pwd"] = kwargs.get("private_key_passphrase")
        return config
