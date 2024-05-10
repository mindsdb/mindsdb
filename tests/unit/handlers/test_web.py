import unittest
from mindsdb.integrations.libs.api_handler_exceptions import TableAlreadyExists
from mindsdb.integrations.handlers.web_handler.web_handler import WebHandler
from mindsdb.integrations.handlers.web_handler.web_handler import CrawlerTable
from mindsdb.integrations.handlers.web_handler import urlcrawl_helpers as helpers
from unittest.mock import patch, MagicMock
import concurrent.futures
import pytest
from bs4 import BeautifulSoup
import pandas as pd


class TestWebsHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.handler = WebHandler(name='test_web_handler')

    def test_crawler_already_registered(self):
        with self.assertRaises(TableAlreadyExists):
            self.handler._register_table('crawler', CrawlerTable)


PDF_CONTENT = (
    b"%PDF-1.7\n\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n\n2 0 obj\n<< /Type /Pages "
    b"/Kids [3 0 R] /Count 1 >>\nendobj\n\n3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R "
    b">>\nendobj\n\n4 0 obj\n<< /Length 22 >>\nstream\nBT\n/Helvetica 12 Tf\n1 0 0 1 50 700 Tm\n("
    b"Hello, this is a test!) Tj\nET\nendstream\nendobj\n\nxref\n0 5\n0000000000 65535 "
    b"f\n0000000010 00000 n\n0000000077 00000 n\n0000000122 00000 n\n0000000203 00000 n\n0000000277 "
    b"00000 n\ntrailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n343\n%%EOF\n "
)

BROKEN_PDF_CONTENT = b"%PDF-1.4\n\nThis is not a valid PDF file content\n"

HTML_SAMPLE_1 = "<h1>Heading One</h1><h2>Heading Two</h2>"

MARKDOWN_SAMPLE_1 = "# Heading One \n\n ## Heading Two"


class TestPDFToMarkdownTest:
    @patch("requests.Response")
    def test_pdf_to_markdown(self, mock_response) -> None:
        response = mock_response.return_value
        response.content = PDF_CONTENT
        result = helpers.pdf_to_markdown(response)
        assert "Hello, this is a test!" in result

    @patch("requests.Response")
    def test_broken_pdf_to_markdown(self, mock_response) -> None:
        response = mock_response.return_value
        response.content = BROKEN_PDF_CONTENT

        with pytest.raises(Exception, match="Failed to process PDF data"):
            helpers.pdf_to_markdown(response)


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
    assert helpers.is_valid(url) == result


@pytest.mark.parametrize(
    "html, markdown",
    [(HTML_SAMPLE_1, MARKDOWN_SAMPLE_1)],
)
def test_get_readable_text_from_soup(html: str, markdown: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    import re
    expected = re.sub(r'\s+', ' ', markdown).strip()
    actual = re.sub(r'\s+', ' ', helpers.get_readable_text_from_soup(soup)).strip()

    assert expected == actual


@patch("mindsdb.integrations.handlers.web_handler.urlcrawl_helpers.get_all_website_links")
@patch("concurrent.futures.ProcessPoolExecutor")
def test_parallel_get_all_website_links(mock_executor, mock_get_links):
    # Setup: Mock the get_all_website_links function to return a list of links
    mock_get_links.return_value = ["link1", "link2", "link3"]

    # Setup: Mock the ProcessPoolExecutor class to return a mock executor
    mock_executor_instance = MagicMock()
    mock_executor.return_value.__enter__.return_value = mock_executor_instance

    # Setup: Mock the executor to return a future that immediately completes with a result
    mock_future = concurrent.futures.Future()
    mock_future.set_result(["link1", "link2", "link3"])
    mock_executor_instance.submit.return_value = mock_future

    # Call the function with a list of URLs
    urls = ["url1", "url2", "url3"]
    result = helpers.parallel_get_all_website_links(urls)

    # Assert: Check if the function returns the expected result
    expected = {
        "url1": ["link1", "link2", "link3"],
        "url2": ["link1", "link2", "link3"],
        "url3": ["link1", "link2", "link3"],
    }
    assert result == expected

    # Assert: Check if the mocks were called as expected
    mock_get_links.assert_called()


def test_dict_to_dataframe():
    # Setup: Create a dictionary of dictionaries
    data = {
        "row1": {"column1": 1, "column2": 2, "column3": 3},
        "row2": {"column1": 4, "column2": 5, "column3": 6},
        "row3": {"column1": 7, "column2": 8, "column3": 9},
    }

    # Call the function with the data, ignoring "column2" and setting the index name to "ID"
    df = helpers.dict_to_dataframe(data, columns_to_ignore=["column2"], index_name="ID")

    # Assert: Check if the DataFrame has the expected structure
    expected = pd.DataFrame({
        "column1": {"row1": 1, "row2": 4, "row3": 7},
        "column3": {"row1": 3, "row2": 6, "row3": 9},
    })
    expected.index.name = "ID"
    pd.testing.assert_frame_equal(df, expected)
