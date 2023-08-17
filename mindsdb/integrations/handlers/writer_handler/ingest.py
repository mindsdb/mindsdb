import time

import pandas as pd
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Chroma

from mindsdb.utilities import log

from .settings import (
    DEFAULT_EMBEDDINGS_MODEL,
    df_to_documents,
    get_chroma_settings,
    load_embeddings_model,
)

logger = log.getLogger(__name__)

class Ingestor:
    def __init__(self, args: dict, df: pd.DataFrame):
        self.args = args
        self.df = df
        self.embeddings_model_name = args.get(
            "embeddings_model_name", DEFAULT_EMBEDDINGS_MODEL
        )
        self.persist_directory = args["chromadb_storage_path"]

        self.chroma_settings = get_chroma_settings(self.persist_directory)

    def split_documents(self):
        # Load documents and split in chunks
        logger.info(f"Loading documents from input data")

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
        documents = df_to_documents(
            df=self.df, page_content_columns=self.args["context_columns"]
        )
        texts = text_splitter.split_documents(documents)
        logger.info(f"Loaded {len(documents)} documents from input data")
        logger.info(f"Split into {len(texts)} chunks of text (max. 500 tokens each)")

        return texts

    def embeddings_to_vectordb(self):
        start_time = time.time()

        # Load documents and split in chunks
        texts = self.split_documents()

        # Load embeddings model
        embeddings_model = load_embeddings_model(self.embeddings_model_name)

        logger.info(f"Creating vectorstore from documents")

        # Create and store locally vectorstore
        db = Chroma.from_documents(
            texts,
            embedding=embeddings_model,
            persist_directory=self.persist_directory,
            client_settings=self.chroma_settings,
            collection_name=self.args.get("collection_name", "langchain"),
        )
        db.persist()
        db = None
        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(
            "Fished creating vectorstore from documents. It took: {elapsed_time/60} minutes"
        )

        logger.info("Finished creating vectorstore from documents.")
        logger.info(f"Elapsed time: {round(elapsed_time / 60)} minutes")
