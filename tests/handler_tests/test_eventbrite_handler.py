from mindsdb.integrations.handlers.eventbrite_handler.eventbrite_handler import (
    EventbriteHandler,
)

from mindsdb.integrations.handlers.eventbrite_handler.eventbrite_handler import (
    CategoryInfoTable,
    EventDetailsTable,
)

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from unittest.mock import Mock

import pandas as pd
import unittest


class CategoryInfoTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(EventbriteHandler)
        trades_table = CategoryInfoTable(api_handler)
        # Order matters.
        expected_columns = [
            "resource_uri",
            "id",
            "name",
            "name_localized",
            "short_name",
            "short_name_localized",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(EventbriteHandler)
        api_handler.api.return_value = pd.DataFrame(
            [
                [
                    "https://www.eventbriteapi.com/v3/categories/103/",  # resource_uri
                    "103",  # id
                    "Music",  # name
                    "Music",  # name_localized
                ]
            ]
        )
        eventbrite_table = api_handler

        select_all = ast.Select(
            targets=[Star()],
            from_table="categoryInfoTable",
        )

        all_location_data = eventbrite_table.select(select_all)
        first_data = all_location_data.iloc[0]

        self.assertEqual(
            first_data["resource_uri"],
            "https://www.eventbriteapi.com/v3/categories/103/",
        )
        self.assertEqual(first_data["id"], "103")
        self.assertEqual(first_data["name"], "Music")
        self.assertEqual(first_data["name_localized"], "Music")


class EventDetailsTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(EventbriteHandler)
        trades_table = EventDetailsTable(api_handler)
        # Order matters.
        expected_columns = [
            "name_text",
            "name_html",
            "description_text",
            "description_html",
            "url",
            "start_timezone",
            "start_local",
            "start_utc",
            "end_timezone",
            "end_local",
            "end_utc",
            "organization_id",
            "created",
            "changed",
            "published",
            "capacity",
            "capacity_is_custom",
            "status",
            "currency",
            "listed",
            "shareable",
            "online_event",
            "tx_time_limit",
            "hide_start_date",
            "hide_end_date",
            "locale",
            "is_locked",
            "privacy_setting",
            "is_series",
            "is_series_parent",
            "inventory_type",
            "is_reserved_seating",
            "show_pick_a_seat",
            "show_seatmap_thumbnail",
            "show_colors_in_seatmap_thumbnail",
            "source",
            "is_free",
            "version",
            "summary",
            "facebook_event_id",
            "logo_id",
            "organizer_id",
            "venue_id",
            "category_id",
            "subcategory_id",
            "format_id",
            "id",
            "resource_uri",
            "is_externally_ticketed",
            "logo_crop_mask",
            "logo_original",
            "logo_id",
            "logo_url",
            "logo_aspect_ratio",
            "logo_edge_color",
            "logo_edge_color_set",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(EventbriteHandler)
        api_handler.api.return_value = pd.DataFrame(
            [
                [
                    "AI Forum: Can AI Fix Climate Change?",  # name_text
                    "The third in a series of lunchtime presentations by King's researchers at Science Gallery London",  # description_text
                    "https://www.eventbrite.co.uk/e/ai-forum-can-ai-fix-climate-change-tickets-717926867587",  # url
                ]
            ]
        )
        eventbrite_table = EventDetailsTable(api_handler)

        name_text_identifier = Identifier(path_str="name_text")
        description_text_identifier = Identifier(path_str="description_text")
        url_identifier = Identifier(path_str="url")

        select_all = ast.Select(
            targets=[name_text_identifier, description_text_identifier, url_identifier],
            from_table="eventDetailsTable",
            where='locationId = "717926867587"',
        )

        review_data = eventbrite_table.select(select_all)
        first_data = review_data.iloc[0]

        self.assertEqual(review_data.shape[1], 3)
        self.assertEqual(
            first_data["name_text"], "AI Forum: Can AI Fix Climate Change?"
        )
        self.assertEqual(
            first_data["description_text"],
            "The third in a series of lunchtime presentations by King's researchers at Science Gallery London",
        )
        self.assertEqual(
            first_data["url"],
            "https://www.eventbrite.co.uk/e/ai-forum-can-ai-fix-climate-change-tickets-717926867587",
        )
