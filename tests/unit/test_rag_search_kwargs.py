import os
import uuid
import pytest
from unittest.mock import Mock, patch
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.documents import Document
from langchain.vectorstores.base import VectorStore
import tempfile
import shutil
from langchain_community.vectorstores import Chroma
from langchain_text_splitters import RecursiveCharacterTextSplitter

from mindsdb.integrations.utilities.rag.settings import (
    RAGPipelineModel,
    RetrieverType,
    SearchKwargs,
    SearchType,
    MultiVectorRetrieverMode,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_ENDPOINT
)
from mindsdb.integrations.utilities.rag.pipelines.rag import LangChainRAGPipeline

requires_openai = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY environment variable not set"
)


@pytest.fixture
def chat_llm():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY environment variable not set")
    return ChatOpenAI(
        model=DEFAULT_LLM_MODEL,
        openai_api_base=DEFAULT_LLM_ENDPOINT,
        api_key=api_key
    )


@pytest.fixture
def embeddings():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY environment variable not set")
    return OpenAIEmbeddings(api_key=api_key)


class MockVectorStore(VectorStore):
    def add_texts(self, *args, **kwargs):
        pass

    def similarity_search(self, *args, **kwargs):
        pass

    def as_retriever(self, **kwargs):
        return Mock()


@pytest.fixture
def sample_documents():
    return [
        Document(page_content="Test document 1", metadata={"source": "test1"}),
        Document(page_content="Test document 2", metadata={"source": "test2"})
    ]


@pytest.fixture
def vector_store_path():
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def vector_store(embeddings, vector_store_path):
    return Chroma(
        embedding_function=embeddings,
        persist_directory=vector_store_path
    )


@pytest.fixture
def base_config(sample_documents, chat_llm, embeddings, vector_store):
    return RAGPipelineModel(
        documents=sample_documents,
        vector_store=vector_store,
        embedding_model=embeddings,
        llm=chat_llm
    )


class TestRAGSearchKwargs:
    @pytest.fixture(autouse=True)
    def setup(self, base_config, sample_documents, chat_llm, embeddings, vector_store):
        """Setup test configuration with fixtures"""
        self.base_config = base_config
        self.sample_documents = sample_documents
        self.chat_llm = chat_llm
        self.embeddings = embeddings
        self.vector_store = vector_store
        self.base_dict = {
            'documents': self.sample_documents,
            'vector_store': self.vector_store,
            'embedding_model': self.embeddings,
            'llm': self.chat_llm
        }

    @requires_openai
    def test_vector_store_retriever_search_kwargs(self):
        config = RAGPipelineModel(
            **self.base_dict,
            search_type=SearchType.SIMILARITY_SCORE_THRESHOLD,
            search_kwargs=SearchKwargs(
                k=3,
                score_threshold=0.5
            ),
            retriever_type=RetrieverType.VECTOR_STORE
        )
        mock_retriever = Mock()
        mock_retriever.search_kwargs = {"k": 3, "score_threshold": 0.5}
        with patch('mindsdb.integrations.utilities.rag.vector_store.VectorStoreOperator') as mock_vs_op:
            mock_vs_op.return_value.vector_store.as_retriever.return_value = mock_retriever
            _ = LangChainRAGPipeline.from_retriever(config)
            assert mock_retriever.search_kwargs == {"k": 3, "score_threshold": 0.5}

    def test_auto_retriever_search_kwargs(self):
        config = RAGPipelineModel(
            **self.base_dict,
            search_type=SearchType.MMR,
            search_kwargs=SearchKwargs(
                k=2,
                fetch_k=4,
                lambda_mult=0.7
            ),
            retriever_type=RetrieverType.AUTO
        )
        mock_retriever = Mock()
        mock_retriever.search_kwargs = {"k": 2, "fetch_k": 4, "lambda_mult": 0.7}
        mock_llm_response = Mock()
        mock_llm_response.content = '[{"name": "source", "description": "Source field", "type": "string"}]'
        with patch('mindsdb.integrations.utilities.rag.retrievers.auto_retriever.AutoRetriever') as MockAutoRetriever, \
             patch('langchain_openai.chat_models.ChatOpenAI.invoke', return_value=mock_llm_response):
            mock_auto = Mock()
            mock_auto.as_runnable.return_value = mock_retriever
            MockAutoRetriever.return_value = mock_auto
            _ = LangChainRAGPipeline.from_auto_retriever(config)
            assert mock_retriever.search_kwargs == {"k": 2, "fetch_k": 4, "lambda_mult": 0.7}

    def test_search_kwargs_validation(self):
        """Test the validation rules for SearchKwargs"""
        # Test fetch_k validation for MMR search type
        with pytest.raises(ValueError, match="fetch_k must be greater than k"):
            RAGPipelineModel(
                **self.base_dict,
                search_type=SearchType.MMR,
                search_kwargs=SearchKwargs(
                    k=5,
                    fetch_k=3,
                    lambda_mult=0.7
                )
            )

        # Test MMR parameter requirements
        with pytest.raises(ValueError, match="lambda_mult is required when using fetch_k"):
            RAGPipelineModel(
                **self.base_dict,
                search_type=SearchType.MMR,
                search_kwargs=SearchKwargs(
                    k=3,
                    fetch_k=5
                )
            )

        with pytest.raises(ValueError, match="fetch_k is required when using lambda_mult"):
            RAGPipelineModel(
                **self.base_dict,
                search_type=SearchType.MMR,
                search_kwargs=SearchKwargs(
                    k=3,
                    lambda_mult=0.7
                )
            )

        # Test score_threshold requirement for SIMILARITY_SCORE_THRESHOLD
        with pytest.raises(ValueError, match="score_threshold is required"):
            RAGPipelineModel(
                **self.base_dict,
                search_type=SearchType.SIMILARITY_SCORE_THRESHOLD,
                search_kwargs=SearchKwargs(
                    k=3
                )
            )

    def test_search_type_compatibility(self):
        """Test that search kwargs match the search type"""
        # Test MMR search configuration
        config = RAGPipelineModel(
            **self.base_dict,
            search_type=SearchType.MMR,
            search_kwargs=SearchKwargs(
                k=3,
                fetch_k=6,
                lambda_mult=0.7
            )
        )
        assert config.search_kwargs.fetch_k == 6
        assert config.search_kwargs.lambda_mult == 0.7

        # Test similarity_score_threshold configuration
        config = RAGPipelineModel(
            **self.base_dict,
            search_type=SearchType.SIMILARITY_SCORE_THRESHOLD,
            search_kwargs=SearchKwargs(
                k=3,
                score_threshold=0.5
            )
        )
        assert config.search_kwargs.score_threshold == 0.5

        # Test basic similarity configuration
        config = RAGPipelineModel(
            **self.base_dict,
            search_type=SearchType.SIMILARITY,
            search_kwargs=SearchKwargs(
                k=3,
                filter={"source": "test1"}
            )
        )
        assert config.search_kwargs.filter == {"source": "test1"}


def test_multi_vector_retriever_search_kwargs(base_config):
    search_kwargs = SearchKwargs(
        k=5,
        filter={"source": "test1"},
    )
    base_config.search_kwargs = search_kwargs
    base_config.search_type = SearchType.SIMILARITY
    base_config.retriever_type = RetrieverType.MULTI
    base_config.multi_retriever_mode = MultiVectorRetrieverMode.BOTH

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=base_config.chunk_size,
        chunk_overlap=base_config.chunk_overlap
    )
    base_config.text_splitter = text_splitter

    mock_retriever = Mock()
    mock_retriever.search_kwargs = {}

    with patch('mindsdb.integrations.utilities.rag.pipelines.rag.MultiVectorRetriever') as MockMultiRetrieverClass:
        class MockMultiRetriever:
            def __init__(self, config):
                self.text_splitter = text_splitter
                self.documents = config.documents
                self.config = config

            def as_runnable(self):
                return mock_retriever

            def _split_documents(self):
                return [], []

            def _generate_id_and_split_document(self, doc):
                return str(uuid.uuid4()), [doc]

        MockMultiRetrieverClass.side_effect = MockMultiRetriever

        _ = LangChainRAGPipeline.from_multi_vector_retriever(base_config)
        mock_retriever.search_kwargs.update({"k": 5, "filter": {"source": "test1"}})
        assert mock_retriever.search_kwargs == {"k": 5, "filter": {"source": "test1"}}
