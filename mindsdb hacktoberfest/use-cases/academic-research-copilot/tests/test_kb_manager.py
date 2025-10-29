import pytest
from src.knowledge_base.kb_manager import KBManager

@pytest.fixture
def kb_manager():
    return KBManager()

def test_create_knowledge_base(kb_manager):
    result = kb_manager.create_knowledge_base("test_kb")
    assert result is True
    assert "test_kb" in kb_manager.list_knowledge_bases()

def test_populate_knowledge_base(kb_manager):
    kb_manager.create_knowledge_base("test_kb")
    sample_data = [
        {"title": "Sample Paper 1", "abstract": "This is a sample abstract for paper 1."},
        {"title": "Sample Paper 2", "abstract": "This is a sample abstract for paper 2."}
    ]
    result = kb_manager.populate_knowledge_base("test_kb", sample_data)
    assert result is True
    assert kb_manager.get_knowledge_base_size("test_kb") == 2

def test_delete_knowledge_base(kb_manager):
    kb_manager.create_knowledge_base("test_kb")
    result = kb_manager.delete_knowledge_base("test_kb")
    assert result is True
    assert "test_kb" not in kb_manager.list_knowledge_bases()