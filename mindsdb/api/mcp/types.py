from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter


class OkResponse(BaseModel):
    type: Literal["ok"]
    affected_rows: int | None = None


class ErrorResponse(BaseModel):
    type: Literal["error"]
    error_code: int
    error_message: str


class TableResponse(BaseModel):
    type: Literal["table"]
    column_names: list[str]
    data: list[list]


QueryResponseAnswer = Annotated[Union[OkResponse, ErrorResponse, TableResponse], Field(discriminator="type")]

response_adapter = TypeAdapter(QueryResponseAnswer)
