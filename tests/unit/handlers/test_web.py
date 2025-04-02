from urllib.parse import urljoin
import concurrent.futures

import pytest
import unittest
from unittest.mock import patch, MagicMock

from requests import Response, Request
from bs4 import BeautifulSoup

from mindsdb.integrations.libs.api_handler_exceptions import TableAlreadyExists
from mindsdb.integrations.handlers.web_handler.web_handler import WebHandler
from mindsdb.integrations.handlers.web_handler.web_handler import CrawlerTable
from mindsdb.integrations.handlers.web_handler import urlcrawl_helpers as helpers


from mindsdb.integrations.utilities.sql_utils import (FilterCondition, FilterOperator)


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


def html_get(url, **kwargs):
    # generate html page with 10 sub-links in the same domain
    if not url.endswith('/'):
        url = url + '/'
    links = [
        f"<a href='{urljoin(url, str(i))}'>link {i}</a>\n"
        for i in range(10)
    ]

    html = f"""
    <html>
        <body>
            Content for {url}
            {"".join(links)}
            <a href="https://www.google.com/" >different domain</a>
        </body>
    </html>
    """
    resp = Response()
    resp._content = html.encode()
    resp.request = Request()
    resp.status_code = 200

    return resp


class TestWebHandler(unittest.TestCase):

    @patch('requests.Session.get')
    def test_web_cases(self, mock_get):

        mock_get.side_effect = html_get

        crawler_table = CrawlerTable(handler=MagicMock())

        # filters
        single_url = FilterCondition('url', FilterOperator.EQUAL, 'https://docs.mindsdb.com/')
        two_urls = FilterCondition('url', FilterOperator.IN, ('https://docs.mindsdb.com/', 'https://docs.python.org/'))

        depth_0 = FilterCondition('crawl_depth', FilterOperator.EQUAL, 0)
        depth_1 = FilterCondition('crawl_depth', FilterOperator.EQUAL, 1)
        depth_2 = FilterCondition('crawl_depth', FilterOperator.EQUAL, 2)

        per_url_2 = FilterCondition('per_url_limit', FilterOperator.EQUAL, 2)

        # ---- single url -----

        # default limit 1
        df = crawler_table.list(conditions=[single_url])
        assert len(df) == 1

        # requested count of results
        df = crawler_table.list(conditions=[single_url], limit=100)
        assert len(df) == 100

        # ---- depth -----

        # only main url
        df = crawler_table.list(conditions=[single_url, depth_0])
        assert len(df) == 1

        # main url and all links from it
        df = crawler_table.list(conditions=[single_url, depth_1])
        assert len(df) == 11

        # main url, +10 from it, +10*10 from every nested
        df = crawler_table.list(conditions=[single_url, depth_2])
        assert len(df) == 111

        # depth + limit
        df = crawler_table.list(conditions=[single_url, depth_2], limit=5)
        assert len(df) == 5

        # ---- multiple url -----

        # without limit: every url
        df = crawler_table.list(conditions=[two_urls])
        assert len(df) == 2

        # with limit: as requested
        df = crawler_table.list(conditions=[two_urls], limit=100)
        assert len(df) == 100

        # every url twice
        df = crawler_table.list(conditions=[two_urls, per_url_2])
        assert len(df) == 4

        # every url twice, limited
        df = crawler_table.list(conditions=[two_urls, per_url_2], limit=3)
        assert len(df) == 3

        # ---- multiple + depth -----

        # one result per url
        df = crawler_table.list(conditions=[two_urls, depth_0])
        assert len(df) == 2

        # crawl 2 levels both urls
        df = crawler_table.list(conditions=[two_urls, depth_2])
        assert len(df) == 2 * 111

        # ---- multiple + depth + limit -----

        # 2 levels, limited
        df = crawler_table.list(conditions=[two_urls, depth_2], limit=100)
        assert len(df) == 100

        # ---- multiple + depth + per_url -----

        # one result per url
        df = crawler_table.list(conditions=[two_urls, depth_0, per_url_2])
        assert len(df) == 2

        # two pages per url
        df = crawler_table.list(conditions=[two_urls, depth_2, per_url_2])
        assert len(df) == 4

        # ---- multiple + depth + per_url + limit

        # one result per url
        df = crawler_table.list(conditions=[two_urls, depth_0, per_url_2], limit=3)
        assert len(df) == 2

        # 4 results but limited
        df = crawler_table.list(conditions=[two_urls, depth_2, per_url_2], limit=3)
        assert len(df) == 3
