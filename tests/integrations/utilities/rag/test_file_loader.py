from mindsdb.integrations.utilities.rag.loaders.file_loader import FileLoader


def test_load_pdf():
    loader = FileLoader('./tests/integrations/utilities/rag/data/test.pdf')
    docs = loader.load()
    # Each page is a doc.
    assert len(docs) == 3
    assert 'THE CASE FOR MACHINE LEARNING' in docs[0].page_content
    assert 'INTRODUCTION' in docs[1].page_content
    assert 'THE CASE FOR \nDEMOCRATIZING \nMACHINE LEARNING' in docs[2].page_content


def test_load_csv():
    loader = FileLoader('./tests/integrations/utilities/rag/data/movies.csv')
    docs = loader.load()
    # Each row is a doc.
    assert len(docs) == 10
    assert 'Toy Story' in docs[0].page_content
    assert 'GoldenEye' in docs[9].page_content


def test_load_html():
    loader = FileLoader('./tests/integrations/utilities/rag/data/test.html')
    docs = loader.load()
    assert len(docs) == 1
    assert 'Some intro text about Foo' in docs[0].page_content


def test_load_md():
    loader = FileLoader('./mindsdb/integrations/handlers/langchain_handler/README.md')
    docs = loader.load()
    assert len(docs) == 1
    assert 'This documentation describes the integration of MindsDB with LangChain' in docs[0].page_content


def test_load_text():
    loader = FileLoader('./tests/integrations/utilities/rag/data/test.txt')
    docs = loader.load()
    assert len(docs) == 1
    assert 'This is a test plaintext file' in docs[0].page_content
