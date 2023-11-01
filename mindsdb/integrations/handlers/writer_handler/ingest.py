import pandas as pd

from mindsdb.integrations.handlers.rag_handler.ingest import RAGIngestor
from mindsdb.integrations.handlers.writer_handler.settings import (
    WriterHandlerParameters,
)


class WriterIngestor(RAGIngestor):
    def __init__(self, args: WriterHandlerParameters, df: pd.DataFrame):
        super().__init__(args, df)
