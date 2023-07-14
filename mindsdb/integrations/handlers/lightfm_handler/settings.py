from pydantic import BaseModel


class ModelParameters(BaseModel):
    learning_rate: float = 0.05
    loss: str = "warp"
    epochs: int = 10
