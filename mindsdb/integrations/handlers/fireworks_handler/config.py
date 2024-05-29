from pydantic import BaseModel, Extra


class FireworksHandlerArgs(BaseModel):
    target: str = None
    model: str = None
    mode: str = None
    max_tokens: int = None
    column: str = None

    class Config:
        # For all args that are not expected, raise an error
        extra = Extra.forbid

