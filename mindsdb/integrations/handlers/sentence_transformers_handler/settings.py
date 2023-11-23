from pydantic import BaseModel, Extra

DEFAULT_EMBEDDING_MODEL = "all-MiniLM-L6-v2"


class Parameters(BaseModel):
    embeddings_model_name: str = DEFAULT_EMBEDDING_MODEL
    text_columns: list = None
    use_gpu: bool = False

    class Config:
        extra = Extra.forbid
