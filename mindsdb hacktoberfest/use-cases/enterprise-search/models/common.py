from typing import Optional
from pydantic import BaseModel, Field


class Filter(BaseModel):
    filters: Optional[dict] = Field(
        default_factory=dict, description="Filters to apply to the data"
    )
