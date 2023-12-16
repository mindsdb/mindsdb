import difflib

from pydantic import BaseModel, Extra, root_validator


class ChromaHandlerConfig(BaseModel):
    """
    Configuration for VectorStoreHandler.
    """

    vector_store: str
    persist_directory: str = None
    host: str = None
    port: str = None
    password: str = None

    class Config:
        extra = Extra.forbid

    @root_validator(pre=True, allow_reuse=True, skip_on_failure=True)
    def check_param_typos(cls, values):
        """Check if there are any typos in the parameters."""

        expected_params = cls.__fields__.keys()
        for key in values.keys():
            if key not in expected_params:
                close_matches = difflib.get_close_matches(
                    key, expected_params, cutoff=0.4
                )
                if close_matches:
                    raise ValueError(
                        f"Unexpected parameter '{key}'. Did you mean '{close_matches[0]}'?"
                    )
                else:
                    raise ValueError(f"Unexpected parameter '{key}'.")
        return values

    @root_validator(allow_reuse=True, skip_on_failure=True)
    def check_config(cls, values):
        """Check if config is valid."""

        vector_store = values.get("vector_store")
        host = values.get("host")
        port = values.get("port")
        persist_directory = values.get("persist_directory")

        if bool(port) != bool(host) or (host and persist_directory):
            raise ValueError(
                f"For {vector_store} handler - host and port must be provided together. "
                f"Additionally, if host and port are provided, persist_directory should not be provided."
            )

        if persist_directory and (host or port):
            raise ValueError(
                f"For {vector_store} handler - if persistence_folder is provided, "
                f"host, port should not be provided."
            )

        return values
