from typing import List, Union
from pydantic import BaseModel, Extra, field_validator

from mindsdb.integrations.handlers.rag_handler.settings import (
    RAGBaseParameters,
)
from langchain_core.callbacks import StreamingStdOutCallbackHandler

EVAL_COLUMN_NAMES = (
    "question",
    "answers",
    "context",
)

SUPPORTED_EVALUATION_TYPES = ("retrieval", "e2e")

GENERATION_METRICS = ("rouge", "meteor", "cosine_similarity", "accuracy")
RETRIEVAL_METRICS = ("cosine_similarity", "accuracy")


# todo make a separate class for evaluation parameters


class WriterLLMParameters(BaseModel):
    """Model parameters for the Writer LLM API interface"""

    writer_api_key: str
    writer_org_id: str = None
    base_url: str = None
    model_id: str = "palmyra-x"
    callbacks: List[StreamingStdOutCallbackHandler] = [StreamingStdOutCallbackHandler()]
    max_tokens: int = 1024
    temperature: float = 0.0
    top_p: float = 1
    stop: List[str] = []
    best_of: int = 5
    verbose: bool = False

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True


class WriterHandlerParameters(RAGBaseParameters):
    """Model parameters for create model"""

    llm_params: WriterLLMParameters
    generation_evaluation_metrics: List[str] = list(GENERATION_METRICS)
    retrieval_evaluation_metrics: List[str] = list(RETRIEVAL_METRICS)
    evaluation_type: str = "e2e"
    n_rows_evaluation: int = None  # if None, evaluate on all rows
    retriever_match_threshold: float = 0.7
    generator_match_threshold: float = 0.8
    evaluate_dataset: Union[List[dict], str] = "squad_v2_val_100_sample"

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True

    @field_validator("generation_evaluation_metrics")
    def generation_evaluation_metrics_must_be_supported(cls, v):
        for metric in v:
            if metric not in GENERATION_METRICS:
                raise ValueError(
                    f"generation_evaluation_metrics must be one of {', '.join(str(v) for v in GENERATION_METRICS)}, got {metric}"
                )
        return v

    @field_validator("retrieval_evaluation_metrics")
    def retrieval_evaluation_metrics_must_be_supported(cls, v):
        for metric in v:
            if metric not in GENERATION_METRICS:
                raise ValueError(
                    f"retrieval_evaluation_metrics must be one of {', '.join(str(v) for v in RETRIEVAL_METRICS)}, got {metric}"
                )
        return v

    @field_validator("evaluation_type")
    def evaluation_type_must_be_supported(cls, v):
        if v not in SUPPORTED_EVALUATION_TYPES:
            raise ValueError(
                f"evaluation_type must be one of `retrieval` or `e2e`, got {v}"
            )
        return v
