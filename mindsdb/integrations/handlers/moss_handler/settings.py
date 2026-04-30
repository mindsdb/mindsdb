import difflib
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class MossHandlerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    vector_store: str
    project_id: str
    project_key: str
    alpha: float = 0.8

    @model_validator(mode="before")
    @classmethod
    def check_param_typos(cls, values: Any) -> Any:
        expected = set(cls.model_fields.keys())
        for key in values.keys():
            if key not in expected:
                close = difflib.get_close_matches(key, expected, cutoff=0.4)
                hint = f" Did you mean '{close[0]}'?" if close else ""
                raise ValueError(f"Unexpected parameter '{key}'.{hint}")
        return values

    @field_validator("alpha", mode="before")
    @classmethod
    def validate_alpha(cls, v: Any) -> float:
        v = float(v)
        if not 0.0 <= v <= 1.0:
            raise ValueError("alpha must be between 0.0 and 1.0")
        return v
