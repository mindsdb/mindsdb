from mindsdb.integrations.handlers.eventbrite_handler.eventbrite_handler import (
    EventbriteHandler,
)

from mindsdb.integrations.handlers.eventbrite_handler.eventbrite_handler import (
    CategoryInfoTable,
    EventDetailsTable,
)

from mindsdb_sql.parser import ast
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
        api_handler = Mock()
        api_handler.api.list_categories.return_value = pd.DataFrame(
            {
                "locale": "en_US",
                "categories": [
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/103/",
                        "id": "103",
                        "name": "Music",
                        "name_localized": "Music",
                        "short_name": "Music",
                        "short_name_localized": "Music",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/101/",
                        "id": "101",
                        "name": "Business & Professional",
                        "name_localized": "Business & Professional",
                        "short_name": "Business",
                        "short_name_localized": "Business",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/110/",
                        "id": "110",
                        "name": "Food & Drink",
                        "name_localized": "Food & Drink",
                        "short_name": "Food & Drink",
                        "short_name_localized": "Food & Drink",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/113/",
                        "id": "113",
                        "name": "Community & Culture",
                        "name_localized": "Community & Culture",
                        "short_name": "Community",
                        "short_name_localized": "Community",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/105/",
                        "id": "105",
                        "name": "Performing & Visual Arts",
                        "name_localized": "Performing & Visual Arts",
                        "short_name": "Arts",
                        "short_name_localized": "Arts",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/104/",
                        "id": "104",
                        "name": "Film, Media & Entertainment",
                        "name_localized": "Film, Media & Entertainment",
                        "short_name": "Film & Media",
                        "short_name_localized": "Film & Media",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/108/",
                        "id": "108",
                        "name": "Sports & Fitness",
                        "name_localized": "Sports & Fitness",
                        "short_name": "Sports & Fitness",
                        "short_name_localized": "Sports & Fitness",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/107/",
                        "id": "107",
                        "name": "Health & Wellness",
                        "name_localized": "Health & Wellness",
                        "short_name": "Health",
                        "short_name_localized": "Health",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/102/",
                        "id": "102",
                        "name": "Science & Technology",
                        "name_localized": "Science & Technology",
                        "short_name": "Science & Tech",
                        "short_name_localized": "Science & Tech",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/109/",
                        "id": "109",
                        "name": "Travel & Outdoor",
                        "name_localized": "Travel & Outdoor",
                        "short_name": "Travel & Outdoor",
                        "short_name_localized": "Travel & Outdoor",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/111/",
                        "id": "111",
                        "name": "Charity & Causes",
                        "name_localized": "Charity & Causes",
                        "short_name": "Charity & Causes",
                        "short_name_localized": "Charity & Causes",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/114/",
                        "id": "114",
                        "name": "Religion & Spirituality",
                        "name_localized": "Religion & Spirituality",
                        "short_name": "Spirituality",
                        "short_name_localized": "Spirituality",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/115/",
                        "id": "115",
                        "name": "Family & Education",
                        "name_localized": "Family & Education",
                        "short_name": "Family & Education",
                        "short_name_localized": "Family & Education",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/116/",
                        "id": "116",
                        "name": "Seasonal & Holiday",
                        "name_localized": "Seasonal & Holiday",
                        "short_name": "Holiday",
                        "short_name_localized": "Holiday",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/112/",
                        "id": "112",
                        "name": "Government & Politics",
                        "name_localized": "Government & Politics",
                        "short_name": "Government",
                        "short_name_localized": "Government",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/106/",
                        "id": "106",
                        "name": "Fashion & Beauty",
                        "name_localized": "Fashion & Beauty",
                        "short_name": "Fashion",
                        "short_name_localized": "Fashion",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/117/",
                        "id": "117",
                        "name": "Home & Lifestyle",
                        "name_localized": "Home & Lifestyle",
                        "short_name": "Home & Lifestyle",
                        "short_name_localized": "Home & Lifestyle",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/118/",
                        "id": "118",
                        "name": "Auto, Boat & Air",
                        "name_localized": "Auto, Boat & Air",
                        "short_name": "Auto, Boat & Air",
                        "short_name_localized": "Auto, Boat & Air",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/119/",
                        "id": "119",
                        "name": "Hobbies & Special Interest",
                        "name_localized": "Hobbies & Special Interest",
                        "short_name": "Hobbies",
                        "short_name_localized": "Hobbies",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/199/",
                        "id": "199",
                        "name": "Other",
                        "name_localized": "Other",
                        "short_name": "Other",
                        "short_name_localized": "Other",
                    },
                    {
                        "resource_uri": "https://www.eventbriteapi.com/v3/categories/120/",
                        "id": "120",
                        "name": "School Activities",
                        "name_localized": "School Activities",
                        "short_name": "School Activities",
                        "short_name_localized": "School Activities",
                    },
                ],
            }
        )
        eventbrite_table = CategoryInfoTable(api_handler)

        resource_uri_identifier = Identifier(path_str="resource_uri")
        id_identifier = Identifier(path_str="id")
        name_identifier = Identifier(path_str="name")
        name_localized_identifier = Identifier(path_str="name_localized")

        select_all = ast.Select(
            targets=[
                resource_uri_identifier,
                id_identifier,
                name_identifier,
                name_localized_identifier,
            ],
            from_table="categoryInfoTable",
        )

        all_category_data = eventbrite_table.select(select_all)

        self.assertEqual(all_category_data.shape[0], 20)
        self.assertEqual(all_category_data.shape[1], 4)


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
        api_handler = Mock()
        api_handler.api.get_event.return_value = pd.DataFrame(
            {
                "name": {
                    "text": "AI Forum: Can AI Fix Climate Change?",
                    "html": "AI Forum: Can AI Fix Climate Change?",
                },
                "description": {
                    "text": "The third in a series of lunchtime presentations by King's researchers at Science Gallery London",
                    "html": "The third in a series of lunchtime presentations by King's researchers at Science Gallery London",
                },
                "url": "https://www.eventbrite.co.uk/e/ai-forum-can-ai-fix-climate-change-tickets-717926867587",
                "start": {
                    "timezone": "Europe/London",
                    "local": "2023-11-01T13:15:00",
                    "utc": "2023-11-01T13:15:00Z",
                },
                "end": {
                    "timezone": "Europe/London",
                    "local": "2023-11-01T14:00:00",
                    "utc": "2023-11-01T14:00:00Z",
                },
                "organization_id": "112948679745",
                "created": "2023-09-12T15:38:12Z",
                "changed": "2023-10-11T20:01:50Z",
                "published": "2023-09-12T15:44:26Z",
                "capacity": None,
                "capacity_is_custom": None,
                "status": "live",
                "currency": "GBP",
                "listed": True,
                "shareable": True,
                "online_event": False,
                "tx_time_limit": 1200,
                "hide_start_date": False,
                "hide_end_date": False,
                "locale": "en_GB",
                "is_locked": False,
                "privacy_setting": "unlocked",
                "is_series": False,
                "is_series_parent": False,
                "inventory_type": "limited",
                "is_reserved_seating": False,
                "show_pick_a_seat": False,
                "show_seatmap_thumbnail": False,
                "show_colors_in_seatmap_thumbnail": False,
                "source": "coyote",
                "is_free": True,
                "version": None,
                "summary": "The third in a series of lunchtime presentations by King's researchers at Science Gallery London",
                "facebook_event_id": None,
                "logo_id": "596060689",
                "organizer_id": "7201076133",
                "venue_id": "173614819",
                "category_id": "102",
                "subcategory_id": None,
                "format_id": "2",
                "id": "717926867587",
                "resource_uri": "https://www.eventbriteapi.com/v3/events/717926867587/",
                "is_externally_ticketed": False,
                "logo": {
                    "crop_mask": {
                        "top_left": {"x": 0, "y": 0},
                        "width": 2160,
                        "height": 1080,
                    },
                    "original": {
                        "url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F596060689%2F112948679745%2F1%2Foriginal.20230912-154003?auto=format%2Ccompress&q=75&sharp=10&s=0bb5e81d009275e956f98c418a9a3025",
                        "width": 2160,
                        "height": 1080,
                    },
                    "id": "596060689",
                    "url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F596060689%2F112948679745%2F1%2Foriginal.20230912-154003?h=200&w=450&auto=format%2Ccompress&q=75&sharp=10&rect=0%2C0%2C2160%2C1080&s=282ef80475d468edff954cb243435432",
                    "aspect_ratio": "2",
                    "edge_color": "#d6b4be",
                    "edge_color_set": True,
                },
            }
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


if __name__ == "__main__":
    unittest.main()
