import difflib

from pydantic import BaseModel, Extra, root_validator


class WeaviateHandlerConfig(BaseModel):
    """
    Configuration for the Weaviate Handler.
    """

    vector_store: str
    persist_directory: str = None
    url: str = None
    api_key: str = None

    class Config:
        extra = Extra.forbid

    @root_validator(pre=True, allow_reuse=True)
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

    @root_validator(allow_reuse=True)
    def check_config(cls, values):
        """Check if config is valid."""

        vector_store = values.get("vector_store")
        url = values.get("url")
        api_key = values.get("api_key")
        persist_directory = values.get("persist_directory")

        if bool(url) != bool(api_key) or (url and persist_directory):
            raise ValueError(
                f"For {vector_store} handler - url and api_key must be provided together. "
                f"Additionally, if url and api_key are provided, persist_directory should not be provided."
            )

        if persist_directory and (url or api_key):
            raise ValueError(
                f"For {vector_store} handler - if persist_directory is provided, "
                f"url, api_key should not be provided."
            )

        return values