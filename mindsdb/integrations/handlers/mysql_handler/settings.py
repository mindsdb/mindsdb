from pydantic import BaseModel, AnyUrl, model_validator
from urllib.parse import urlparse


class ConnectionConfig(BaseModel):
    # TODO: For now validate AnyURL since MySQLDsn wasn't working
    url: AnyUrl = None
    host: str = None
    port: int = 3306
    user: str = None
    password: str = None
    database: str = None

    @model_validator(mode="before")
    def check_db_params(cls, values):
        """Ensures either URL is provided or all individual parameters are provided."""
        url = values.get("url")
        host = values.get("host")
        user = values.get("user")
        password = values.get("password")
        database = values.get("database")
        if not url and not (host and user and password and database):
            raise ValueError(
                "Either a valid URL or required parameters (host, user, password, database) must be provided."
            )

        if url:
            parsed = urlparse(url)
            values["host"] = parsed.hostname or host
            values["port"] = parsed.port if parsed.port is not None else 3306
            values["user"] = parsed.username or user
            values["password"] = parsed.password or password
            values["database"] = parsed.path[1:] if parsed.path else database

            # mysql connector raise error if url is provided
            values.pop("url", None)

            return values

        if not url:
            for param in ["host", "user", "password", "database"]:
                if not values.get(param):
                    raise ValueError(f"{param} is required when URL is not provided.")
        return values

    class Config:
        str_min_length = 1
        str_strip_whitespace = True
