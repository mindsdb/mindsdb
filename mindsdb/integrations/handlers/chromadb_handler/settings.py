import difflib
from typing import Any

from pydantic import BaseModel, model_validator


class ChromaHandlerConfig(BaseModel):
    """
    Configuration for VectorStoreHandler.
    """

    vector_store: str
    persist_directory: str = None
    host: str = None
    port: str = None
    password: str = None
    distance: str = "cosine"
    tenant: str = None
    database: str = None
    api_key: str = None
    ssl: bool = False

    class Config:
        extra = "forbid"

    @model_validator(mode="before")
    @classmethod
    def check_param_typos(cls, values: Any) -> Any:
        """Check if there are any typos in the parameters."""

        expected_params = cls.model_fields.keys()
        for key in values.keys():
            if key not in expected_params:
                close_matches = difflib.get_close_matches(key, expected_params, cutoff=0.4)
                if close_matches:
                    raise ValueError(f"Unexpected parameter '{key}'. Did you mean '{close_matches[0]}'?")
                else:
                    raise ValueError(f"Unexpected parameter '{key}'.")
        return values

    @model_validator(mode="before")
    @classmethod
    def check_config(cls, values: Any) -> Any:
        """Check if config is valid."""

        vector_store = values.get("vector_store")
        host = values.get("host")
        port = values.get("port")
        persist_directory = values.get("persist_directory")
        api_key = values.get("api_key")
        tenant = values.get("tenant")
        database = values.get("database")

        if bool(port) != bool(host) or (host and persist_directory):
            raise ValueError(
                f"For {vector_store} handler - host and port must be provided together. "
                f"Additionally, if host and port are provided, persist_directory should not be provided."
            )

        if persist_directory and (host or port):
            raise ValueError(
                f"For {vector_store} handler - if persistence_folder is provided, host, port should not be provided."
            )

        # Validate that if api_key is provided, tenant and database are also provided
        if api_key and (not tenant or not database):
            raise ValueError(
                f"For {vector_store} handler - if api_key is provided, both tenant and database must also be provided."
            )

        return values
