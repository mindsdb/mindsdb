from typing import Optional
from pydantic import BaseModel, AnyUrl, TypeAdapter, model_validator, field_validator, ConfigDict
from urllib.parse import urlparse


_ANY_URL_ADAPTER = TypeAdapter(AnyUrl)


class ConnectionConfig(BaseModel):
    """
    MySQL connection configuration with validation.

    Supports two connection methods:
    1. URL-based: mysql://user:password@host:port/database
    2. Parameter-based: individual host, port, user, password, database params
    """

    url: Optional[AnyUrl] = None
    host: Optional[str] = None
    port: int = 3306
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate that port is within valid range."""
        if v < 1 or v > 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v

    @field_validator("url", mode="before")
    @classmethod
    def validate_url(cls, v: Optional[str]) -> Optional[AnyUrl]:
        """Validate URL using AnyUrl as a fallback option for MySQL DSN parsing."""
        if v is None or isinstance(v, AnyUrl):
            return v
        try:
            return _ANY_URL_ADAPTER.validate_python(v)
        except ValueError as exc:
            raise ValueError(f"Invalid MySQL connection URL: {v}") from exc

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: Optional[str]) -> Optional[str]:
        """Validate that host is not empty if provided."""
        if v is not None and not v.strip():
            raise ValueError("Host cannot be empty string")
        return v

    @field_validator("database")
    @classmethod
    def validate_database(cls, v: Optional[str]) -> Optional[str]:
        """Validate that database name is not empty if provided."""
        if v is not None and not v.strip():
            raise ValueError("Database name cannot be empty string")
        return v

    @model_validator(mode="before")
    @classmethod
    def check_db_params(cls, values):
        """Ensures either URL is provided or all individual parameters are provided."""
        url = values.get("url")
        host = values.get("host")
        user = values.get("user")
        password = values.get("password")
        database = values.get("database")

        if not url and not (host and user and password and database):
            missing_params = []
            if not host:
                missing_params.append("host")
            if not user:
                missing_params.append("user")
            if not password:
                missing_params.append("password")
            if not database:
                missing_params.append("database")

            raise ValueError(
                f"Either a valid URL or all required parameters must be provided. Missing: {', '.join(missing_params)}"
            )

        if url:
            # Parse URL and extract connection parameters
            try:
                parsed = urlparse(str(url))

                # Extract parameters from URL
                values["host"] = parsed.hostname or host
                values["port"] = parsed.port if parsed.port is not None else values.get("port", 3306)
                values["user"] = parsed.username or user
                values["password"] = parsed.password or password
                values["database"] = parsed.path[1:] if parsed.path and len(parsed.path) > 1 else database

                # Validate extracted parameters
                if not values["host"]:
                    raise ValueError("URL must contain a hostname")
                if not values["user"]:
                    raise ValueError("URL must contain a username")
                if not values["database"]:
                    raise ValueError("URL must contain a database name in the path")

            except Exception as e:
                raise ValueError(f"Invalid MySQL connection URL: {str(e)}")

            # mysql connector raises error if url is provided
            values.pop("url", None)

            return values

        # Validate individual parameters
        if not url:
            for param in ["host", "user", "password", "database"]:
                if not values.get(param):
                    raise ValueError(f"'{param}' is required when URL is not provided")

        return values

    model_config = ConfigDict(str_min_length=1, str_strip_whitespace=True, validate_assignment=True)
