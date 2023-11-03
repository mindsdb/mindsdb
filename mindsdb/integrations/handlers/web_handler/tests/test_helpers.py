from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup
from fitz.fitz import FileDataError

from mindsdb.integrations.handlers.web_handler import urlcrawl_helpers as C
from mindsdb.integrations.handlers.web_handler.tests import example_data as D


class TestPDFToMarkdownTest:
    @patch("requests.Response")
    def test_pdf_to_markdown(self, mock_response) -> None:
        response = mock_response.return_value
        response.content = D.PDF_CONTENT
        result = C.pdf_to_markdown(response)
        assert "Hello, this is a test!" in result

    @patch("requests.Response")
    def test_broken_pdf_to_markdown(self, mock_response) -> None:
        response = mock_response.return_value
        response.content = D.BROKEN_PDF_CONTENT

        with pytest.raises(FileDataError) as excinfo:
            C.pdf_to_markdown(response)

        assert str(excinfo.value) == "cannot open broken document"


@pytest.mark.parametrize(
    "url, result",
    [
        ("google", False),
        ("google.com", False),
        ("https://google.com", True),
        ("", False),
    ],
)
def test_url_validation(url: str, result: bool) -> None:
    assert C.is_valid(url) == result


@pytest.mark.parametrize(
    "html, markdown",
    [(D.HTML_SAMPLE_1, D.MARKDOWN_SAMPLE_1), (D.HTML_SAMPLE_2, D.MARKDOWN_SAMPLE_2)],
)
def test_get_readable_text_from_soup(html: str, markdown: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    assert markdown == C.get_readable_text_from_soup(soup)
