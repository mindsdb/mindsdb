import difflib

from pydantic import BaseModel, Extra, root_validator


class WeaviateHandlerConfig(BaseModel):
    """
    Configuration for the Weaviate Handler.
    """

    persist_directory: str = None
    weaviate_url: str = None
    weaviate_api_key: str = None

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

        weaviate_url = values.get("weaviate_url")
        weaviate_api_key = values.get("weaviate_api_key")
        persist_directory = values.get("persist_directory")

        if bool(weaviate_url) != bool(weaviate_api_key):
            raise ValueError(
                "For the Weaviate handler - weaviate_url and weaviate_api_key must be provided together."
            )

        if (weaviate_url and weaviate_api_key) and persist_directory:
            raise ValueError(
                "For the Weaviate handler - either weaviate_url and weaviate_api_key must be provided (together) or persist_directory must be provided. Not both."
            )

        return values
