from unittest.mock import patch

from langchain_core.documents import Document
from langchain_experimental.text_splitter import SemanticChunker
from langchain_text_splitters import MarkdownHeaderTextSplitter, HTMLHeaderTextSplitter, RecursiveCharacterTextSplitter
from mindsdb.integrations.utilities.rag.splitters.file_splitter import FileSplitter, FileSplitterConfig


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
def test_split_documents_pdf(mock_embeddings):
    pdf_doc = Document(
        page_content='This is a test PDF file. Let us try to do some splitting!',
        metadata={'extension': '.pdf'}
    )
    semantic_chunker = SemanticChunker(mock_embeddings)
    file_splitter = FileSplitter(FileSplitterConfig(embeddings=mock_embeddings, semantic_chunker=semantic_chunker))
    split_pdf_docs = file_splitter.split_documents([pdf_doc])
    assert mock_embeddings.embed_documents.called
    assert len(mock_embeddings.embed_documents.call_args.args[0]) > 0
    assert len(split_pdf_docs) > 0


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
def test_split_documents_md(mock_embeddings):
    md_content = '''
    # Unit Testing for Dummies
    This MD document covers how to write basic unit tests.
    ## Introduction
    Unit testing helps ensure code works as expected and prevents regressions. Time to dive in!
    ## How to Write Tests
    To be continued!
'''
    md_doc = Document(
        page_content=md_content,
        metadata={'extension': '.md'}
    )
    headers_to_split_on = [
        ('#', 'Header 1'),
        ('##', 'Header 2'),
    ]
    md_text_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
    file_splitter = FileSplitter(FileSplitterConfig(
        embeddings=mock_embeddings,
        markdown_splitter=md_text_splitter
    ))
    split_md_docs = file_splitter.split_documents([md_doc])
    assert len(split_md_docs) == 3
    # Check we actually split on headers.
    assert 'This MD document covers how to write basic unit tests.' in split_md_docs[0].page_content
    assert 'Unit testing helps ensure code works as expected and prevents regressions. Time to dive in!' in split_md_docs[1].page_content
    assert 'To be continued!' in split_md_docs[2].page_content


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
def test_split_documents_html(mock_embeddings):
    html_content = '''
<!DOCTYPE html>
<html>
<body>
    <div>
        <h1>Foo</h1>
        <p>Some intro text about Foo.</p>
        <div>
            <h2>Bar main section</h2>
            <p>Some intro text about Bar.</p>
            <h3>Bar subsection 1</h3>
            <p>Some text about the first subtopic of Bar.</p>
            <h3>Bar subsection 2</h3>
            <p>Some text about the second subtopic of Bar.</p>
        </div>
        <div>
            <h2>Baz</h2>
            <p>Some text about Baz</p>
        </div>
        <br>
        <p>Some concluding text about Foo</p>
    </div>
</body>
</html>
'''
    headers_to_split_on = [
        ('h1', 'Header 1'),
        ('h2', 'Header 2'),
        ('h3', 'Header 3')
    ]
    html_text_splitter = HTMLHeaderTextSplitter(headers_to_split_on=headers_to_split_on)
    file_splitter = FileSplitter(FileSplitterConfig(
        embeddings=mock_embeddings,
        html_splitter=html_text_splitter
    ))
    html_doc = Document(
        page_content=html_content,
        metadata={'extension': '.html'}
    )
    split_html_docs = file_splitter.split_documents([html_doc])
    assert len(split_html_docs) == 8
    # # Check we actually split on headers.
    assert 'Foo' in split_html_docs[0].page_content
    assert 'Some intro text about Foo' in split_html_docs[1].page_content
    assert 'Some intro text about Bar' in split_html_docs[2].page_content
    assert 'Some text about the first subtopic of Bar' in split_html_docs[3].page_content
    assert 'Some text about the second subtopic of Bar' in split_html_docs[4].page_content
    assert 'Baz' in split_html_docs[5].page_content
    assert 'Some text about Baz' in split_html_docs[6].page_content
    assert 'Some concluding text about Foo' in split_html_docs[7].page_content


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
def test_split_documents_default(mock_embeddings):
    recursive_splitter = RecursiveCharacterTextSplitter()
    file_splitter = FileSplitter(FileSplitterConfig(
        embeddings=mock_embeddings,
        recursive_splitter=recursive_splitter
    ))
    txt_doc = Document(
        page_content='This is a text file!',
        metadata={'extension': '.txt'}
    )
    split_txt_docs = file_splitter.split_documents([txt_doc])
    assert len(split_txt_docs) == 1
    assert 'This is a text file!' in split_txt_docs[0].page_content


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.MarkdownHeaderTextSplitter')
def test_split_documents_failover(mock_embeddings, mock_md_splitter):
    md_content = '''
    # Unit Testing for Dummies
    This MD document covers how to write basic unit tests.
    ## Introduction
    Unit testing helps ensure code works as expected and prevents regressions. Time to dive in!
    ## How to Write Tests
    To be continued!
'''
    mock_md_splitter.split_text.side_effect = Exception('Something went wrong!')
    file_splitter = FileSplitter(FileSplitterConfig(
        embeddings=mock_embeddings,
        markdown_splitter=mock_md_splitter
    ))
    md_doc = Document(
        page_content=md_content,
        metadata={'extension': '.md'}
    )

    # Should throw an exception and go to default.
    split_md_docs = file_splitter.split_documents([md_doc])
    assert len(split_md_docs) > 0


@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.OpenAIEmbeddings')
@patch('mindsdb.integrations.utilities.rag.splitters.file_splitter.MarkdownHeaderTextSplitter')
def test_split_documents_no_failover(mock_embeddings, mock_md_splitter):
    md_content = '''
    # Unit Testing for Dummies
    This MD document covers how to write basic unit tests.
    ## Introduction
    Unit testing helps ensure code works as expected and prevents regressions. Time to dive in!
    ## How to Write Tests
    To be continued!
'''
    mock_md_splitter.split_text.side_effect = Exception('Something went wrong!')
    file_splitter = FileSplitter(FileSplitterConfig(
        embeddings=mock_embeddings,
        markdown_splitter=mock_md_splitter
    ))
    md_doc = Document(
        page_content=md_content,
        metadata={'extension': '.md'}
    )

    # Should throw an exception.
    try:
        _ = file_splitter.split_documents([md_doc], default_failover=False)
    except Exception:
        return
    assert False
