import unittest
from mindsdb.integrations.libs.api_handler_exceptions import TableAlreadyExists
from mindsdb.integrations.handlers.web_handler.web_handler import WebHandler
from mindsdb.integrations.handlers.web_handler.web_handler import CrawlerTable
from mindsdb.integrations.handlers.web_handler import urlcrawl_helpers as helpers
from unittest.mock import patch, MagicMock
import concurrent.futures
import pytest
from bs4 import BeautifulSoup


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


class TestWebHelpers(unittest.TestCase):
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

    def test_url_validation(self):
        assert helpers.is_valid('https://google.com') is True
        assert helpers.is_valid('google.com') is False

    def test_get_readable_text_from_soup(self) -> None:
        soup = BeautifulSoup(HTML_SAMPLE_1, "html.parser")
        import re
        expected = re.sub(r'\s+', ' ', MARKDOWN_SAMPLE_1).strip()
        actual = re.sub(r'\s+', ' ', helpers.get_readable_text_from_soup(soup)).strip()

        assert expected == actual

    @patch("mindsdb.integrations.handlers.web_handler.urlcrawl_helpers.get_all_website_links")
    @patch("concurrent.futures.ProcessPoolExecutor")
    def test_parallel_get_all_website_links(self, mock_executor, mock_get_links):
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


class TestWebHandler(unittest.TestCase):

    @patch('mindsdb.integrations.handlers.web_handler.web_handler.extract_comparison_conditions')
    def test_select_with_or_operator_raise_error(self, mock_extract_comparison_conditions):
        mock_extract_comparison_conditions.return_value = [('or', 'url', 'example.com')]

        crawler_table = CrawlerTable(handler=MagicMock())
        mock_query = MagicMock()
        mock_ast = MagicMock()
        mock_ast.get_type.return_value = 'OR'

        mock_query.where = mock_ast
        with self.assertRaises(NotImplementedError) as context:
            crawler_table.select(mock_query)
        self.assertTrue('OR is not supported' in str(context.exception))

    @patch('mindsdb.integrations.handlers.web_handler.web_handler.extract_comparison_conditions')
    def test_select_with_invalid_url_format(self, mock_extract_comparison_conditions):
        mock_extract_comparison_conditions.return_value = [('WHERE', 'url', 'example.com')]

        crawler_table = CrawlerTable(handler=MagicMock())
        mock_query = MagicMock()
        mock_ast = MagicMock()
        mock_ast.get_type.return_value = 'WHERE URL ("example.com")'

        mock_query.where = mock_ast
        with self.assertRaises(NotImplementedError) as context:
            crawler_table.select(mock_query)
        self.assertTrue('Invalid URL format.' in str(context.exception))

    @patch('mindsdb.integrations.handlers.web_handler.web_handler.extract_comparison_conditions')
    def test_select_with_missing_url_(self, mock_extract_comparison_conditions):
        mock_extract_comparison_conditions.return_value = [('WHERE', 'id', '1')]

        crawler_table = CrawlerTable(handler=MagicMock())
        mock_query = MagicMock()
        mock_ast = MagicMock()
        mock_ast.get_type.return_value = 'WHERE ID ("1")'

        mock_query.where = mock_ast
        with self.assertRaises(NotImplementedError) as context:
            crawler_table.select(mock_query)
        self.assertTrue('You must specify what url you want to craw' in str(context.exception))

    @patch('mindsdb.integrations.handlers.web_handler.web_handler.extract_comparison_conditions')
    def test_select_with_missing_limit(self, mock_extract_comparison_conditions):
        mock_extract_comparison_conditions.return_value = [('=', 'url', 'https://docs.mindsdb.com')]

        crawler_table = CrawlerTable(handler=MagicMock())
        mock_query = MagicMock()
        mock_ast = MagicMock()
        mock_ast.get_type.return_value = 'URL = "https://docs.mindsdb.com"'

        mock_query.where = mock_ast
        mock_query.limit = None
        with self.assertRaises(NotImplementedError) as context:
            crawler_table.select(mock_query)
        self.assertTrue('You must specify a LIMIT clause' in str(context.exception))
