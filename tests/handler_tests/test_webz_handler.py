import unittest
from unittest.mock import Mock

import pandas as pd
from mindsdb_sql import parse_sql

from mindsdb.integrations.handlers.webz_handler.webz_handler import WebzHandler
from mindsdb.integrations.handlers.webz_handler.webz_tables import (
    WebzPostsTable,
    WebzReviewsTable,
)

COLUMNS_POST = [
    "thread__uuid",
    "thread__url",
    "thread__site_full",
    "thread__site",
    "thread__site_section",
    "thread__section_title",
    "thread__title",
    "thread__title_full",
    "thread__published",
    "thread__replies_count",
    "thread__participants_count",
    "thread__site_type",
    "thread__main_image",
    "thread__country",
    "thread__site_categories",
    "thread__social__facebook__likes",
    "thread__social__facebook__shares",
    "thread__social__facebook__comments",
    "thread__social__gplus__shares",
    "thread__social__pinterest__shares",
    "thread__social__linkedin__shares",
    "thread__social__stumbledupon__shares",
    "thread__social__vk__shares",
    "thread__performance_score",
    "thread__domain_rank",
    "thread__domain_rank_updated",
    "thread__reach__per_million",
    "thread__reach__page_views",
    "thread__reach__updated",
    "uuid",
    "url",
    "ord_in_thread",
    "parent_url",
    "author",
    "published",
    "title",
    "text",
    "language",
    "external_links",
    "external_images",
    "rating",
    "entities__persons",
    "entities__organizations",
    "entities__locations",
    "crawled",
]


SAMPLE_POST = {
    "thread__uuid": "e893796adad8a85e6ab5202ac34b5791c8fbb017",
    "thread__url": "https://www.economist.com/business/2023/06/06/generative-ai-could-radically-alter-the-practice-of-law",
    "thread__site_full": "https://www.economist.com",
    "thread__site": "economist.com",
}


class WebzPostsTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        webz_handler = Mock(WebzHandler)
        posts_table = WebzPostsTable(webz_handler)
        # Order matters.
        expected_columns = COLUMNS_POST
        self.assertListEqual(posts_table.get_columns(), expected_columns)

    def test_select_with_query_order_by_and_limit(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_POST])
        posts_table = WebzPostsTable(webz_handler)
        query = parse_sql(
            "SELECT * FROM posts WHERE query='language:english' ORDER BY posts.relevancy LIMIT 10",
            dialect="mindsdb",
        )
        results = posts_table.select(query)
        first_result = results.iloc[0]
        webz_handler.call_webz_api.assert_called_once_with(
            method_name="posts",
            params={
                "q": "language:english",
                "sort": "relevancy",
                "order": "default",
                "size": 10,
            },
        )
        self.assertEqual(results.shape[1], len(COLUMNS_POST))
        for column in COLUMNS_POST:
            self.assertEqual(first_result[column], SAMPLE_POST.get(column, None))

    def test_select_with_targets(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_POST])
        posts_table = WebzPostsTable(webz_handler)
        target_field = "thread__uuid"
        query = parse_sql(
            f"SELECT {target_field} FROM posts",
            dialect="mindsdb",
        )
        results = posts_table.select(query)
        first_result = results.iloc[0]
        webz_handler.call_webz_api.assert_called_once_with(
            method_name="posts",
            params={},
        )
        self.assertEqual(results.shape[1], 1)
        self.assertEqual(first_result[target_field], SAMPLE_POST[target_field])

    def test_select_with_invalid_order_by_field_fails(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_POST])
        posts_table = WebzPostsTable(webz_handler)
        query = parse_sql(
            "SELECT thread__uuid FROM posts ORDER BY posts.invalid_field",
            dialect="mindsdb",
        )
        with self.assertRaises(ValueError) as context:
            posts_table.select(query)
        self.assertEqual(
            str(context.exception), "Order by unknown column invalid_field"
        )


COLUMNS_REVIEW = [
    "item__uuid",
    "item__url",
    "item__site_full",
    "item__site",
    "item__site_section",
    "item__section_title",
    "item__title",
    "item__title_full",
    "item__published",
    "item__reviews_count",
    "item__reviewers_count",
    "item__main_image",
    "item__country",
    "item__site_categories",
    "item__domain_rank",
    "item__domain_rank_updated",
    "uuid",
    "url",
    "ord_in_thread",
    "author",
    "published",
    "title",
    "text",
    "language",
    "external_links",
    "rating",
    "crawled",
]


SAMPLE_REVIEW = {
    "item__uuid": "3bf76e1cee69da4e89ec88d5f0f23d0b66f4b5c6",
    "item__url": "https://www.watsons.com.my/baking-soda-laundry-detergent/p/BP_56634",
    "item__site_full": "www.watsons.com.my",
    "uuid": "d6afd2c04c995f34d9003cea353d7f5e01fcce0b",
}


class WebzReviewsTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        webz_handler = Mock(WebzHandler)
        reviews_table = WebzReviewsTable(webz_handler)
        expected_columns = COLUMNS_REVIEW
        self.assertListEqual(reviews_table.get_columns(), expected_columns)

    def test_select_with_query_order_by_and_limit(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_REVIEW])
        reviews_table = WebzReviewsTable(webz_handler)
        query = parse_sql(
            "SELECT * FROM reviews WHERE query='language:english' ORDER BY reviews.reviews_count LIMIT 10",
            dialect="mindsdb",
        )
        results = reviews_table.select(query)
        first_result = results.iloc[0]
        webz_handler.call_webz_api.assert_called_once_with(
            method_name="reviews",
            params={
                "q": "language:english",
                "sort": "reviews_count",
                "order": "default",
                "size": 10,
            },
        )
        self.assertEqual(results.shape[1], len(COLUMNS_REVIEW))
        for column in COLUMNS_REVIEW:
            self.assertEqual(first_result[column], SAMPLE_REVIEW.get(column, None))

    def test_select_with_targets(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_REVIEW])
        reviews_table = WebzReviewsTable(webz_handler)
        target_field = "uuid"
        query = parse_sql(
            f"SELECT {target_field} FROM reviews",
            dialect="mindsdb",
        )
        results = reviews_table.select(query)
        first_result = results.iloc[0]
        webz_handler.call_webz_api.assert_called_once_with(
            method_name="reviews",
            params={},
        )
        self.assertEqual(results.shape[1], 1)
        self.assertEqual(first_result[target_field], SAMPLE_REVIEW[target_field])

    def test_select_with_invalid_order_by_field_fails(self):
        webz_handler = Mock(WebzHandler)
        webz_handler.call_webz_api.return_value = pd.DataFrame([SAMPLE_REVIEW])
        reviews_table = WebzReviewsTable(webz_handler)
        query = parse_sql(
            "SELECT item__uuid FROM reviews ORDER BY reviews.invalid_field",
            dialect="mindsdb",
        )
        with self.assertRaises(ValueError) as context:
            reviews_table.select(query)
        self.assertEqual(
            str(context.exception), "Order by unknown column invalid_field"
        )
