import pytest
import yaml
from langchain_core.documents import Document
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from mindsdb.integrations.utilities.rag.rag_pipeline_builder import RAG
from pathlib import Path

from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_MODEL, RAGPipelineModel

DEFAULT_LLM = ChatOpenAI(model_name=DEFAULT_LLM_MODEL, temperature=0)
DEFAULT_EMBEDDINGS = OpenAIEmbeddings()

path = Path(__file__).parent
config_path = path / "data" / "rag_pipelines"
pipeline_configs = list(config_path.glob('*.yml'))


def create_test_documents():
    return [
        Document(
            page_content="This is a test document",
            metadata={"doc_id": "1"}
        ),
        Document(
            page_content="This is also a test document",
            metadata={"doc_id": "2"}
        ),
        Document(
            page_content="This is another test document",
            metadata={"doc_id": "3"}
        )
    ]


@pytest.fixture(params=pipeline_configs, ids=lambda x: x.stem, scope='module')
def config(request):
    with open(request.param, 'r') as file:
        config = yaml.safe_load(file)
    config['documents'] = create_test_documents()
    config['llm'] = DEFAULT_LLM
    config['embeddings_model'] = DEFAULT_EMBEDDINGS

    return RAGPipelineModel(**config)


def test_rag_pipeline_creation(config):
    rag = RAG(config)
    result = rag.pipeline.invoke('test document')

    assert result is not None
    assert isinstance(result, dict)
    assert all(key in result for key in ['answer', 'context', 'question'])
