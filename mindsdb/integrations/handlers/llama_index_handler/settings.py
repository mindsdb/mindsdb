from typing import List, Optional
from pydantic import BaseModel, field_validator, model_validator
from pydantic_settings import BaseSettings


class LlamaIndexConfig(BaseSettings):
    """
    Model for LlamaIndexHandler settings.

    Attributes:
        default_index_class (str): Default index class.
        supported_index_class (List[str]): Supported index classes.
        default_reader (str): Default reader. Note this is custom data frame reader.
        supported_reader (List[str]): Supported readers.
    """
    DEFAULT_INDEX_CLASS: str = "VectorStoreIndex"
    SUPPORTED_INDEXES: List[str] = ["VectorStoreIndex"]
    DEFAULT_READER: str = "DFReader"
    SUPPORTED_READERS: List[str] = ["DFReader", "SimpleWebPageReader"]


llama_index_config = LlamaIndexConfig()


class LlamaIndexModel(BaseModel):
    """
    Model for LlamaIndexHandler.

    Attributes:
        reader (str): Reader.
        index_class (str): Index class.
        index (Any): Index.
        reader_params (Any): Reader parameters.
        index_params (Any): Index parameters.
    """
    reader: Optional[str] = None
    index_class: Optional[str] = None
    input_column: str
    openai_api_key: Optional[str] = None
    input_column: Optional[str]
    mode: Optional[str] = None
    user_column: Optional[str] = None
    assistant_column: Optional[str] = None

    @field_validator('reader')
    @classmethod
    def validate_reader(cls, value):
        if value not in llama_index_config.SUPPORTED_READERS:
            raise ValueError(f"Reader {value} is not supported.")

        return value

    @field_validator('index_class')
    @classmethod
    def validate_index_class(cls, value):
        if value not in llama_index_config.SUPPORTED_INDEXES:
            raise ValueError(f"Index class {value} is not supported.")

        return value

    @model_validator(mode='after')
    def validate_mode(self):
        if self.mode == "conversational" and not all([self.user_column, self.assistant_column]):
            raise ValueError("Conversational mode requires user_column and assistant_column parameter")

        return self
