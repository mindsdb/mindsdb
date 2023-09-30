from mindsdb.integrations.handlers.tripadvisor_handler.tripadvisor_handler import (
    TripAdvisorHandler,
)

from mindsdb.integrations.handlers.tripadvisor_handler.tripadvisor_table import (
    SearchLocationTable,
    LocationDetailsTable,
)

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star

from unittest.mock import Mock

import pandas as pd
import unittest


class SearchLocationTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        trades_table = SearchLocationTable(api_handler)
        # Order matters.
        expected_columns = [
            "location_id",
            "name",
            "distance",
            "rating",
            "bearing",
            "street1",
            "street2",
            "city",
            "state",
            "country",
            "postalcode",
            "address_string",
            "phone",
            "latitude",
            "longitude",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        api_handler.call_tripadvisor_searchlocation_api.return_value = pd.DataFrame(
            [
                [
                    "186338",  # locationId
                    "London",  # name
                    "United Kingdom",  # country
                    "London England",  # address_string
                ]
            ]
        )
        tripadvisor_table = SearchLocationTable(api_handler)

        select_all = ast.Select(
            targets=[Star()],
            from_table="searchLocationTable",
            where='searchLocationTable.searchQuery = "London"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

        self.assertEqual(all_location_data.shape[1], 4)
        self.assertEqual(first_data["locationId"], "186338")
        self.assertEqual(first_data["name"], "London")
        self.assertEqual(first_data["country"], "United Kingdom")
        self.assertEqual(first_data["address_string"], "London England")


class LocationDetailsTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        trades_table = LocationDetailsTable(api_handler)
        # Order matters.
        expected_columns = [
            "location_id",
            "distance",
            "name",
            "description",
            "web_url",
            "street1",
            "street2",
            "city",
            "state",
            "country",
            "postalcode",
            "address_string",
            "latitude",
            "longitude",
            "timezone",
            "email",
            "phone",
            "website",
            "write_review",
            "ranking_data",
            "rating",
            "rating_image_url",
            "num_reviews",
            "photo_count",
            "see_all_photos",
            "price_level",
            "brand",
            "parent_brand",
            "ancestors",
            "periods",
            "weekday",
            "features",
            "cuisines",
            "amenities",
            "trip_types",
            "styles",
            "awards",
            "neighborhood_info",
            "parent_brand",
            "brand",
            "groups",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        api_handler.call_tripadvisor_searchlocation_api.return_value = pd.DataFrame(
            [
                [
                    "23322232",  # locationId
                    "La Polleria Alicante",  # name
                    "We are revolutionizing Alicante! Come and try the waffle that is on everyone's mouth. Come and get your #pollofre or #pollolo, we are waiting for you!",  # description
                    "https://www.tripadvisor.com/Restaurant_Review-g1064230-d23322232-Reviews-La_Polleria_Alicante-Alicante_Costa_Blanca_Province_of_Alicante_Valencian_Commu.html?m=66827",  # web_url
                ]
            ]
        )

        tripadvisor_table = LocationDetailsTable(api_handler)

        select_all = ast.Select(
            targets=[Star()],
            from_table="locationDetailsTable",
            where='locationDetailsTable.searchQuery = "23322232"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

        self.assertEqual(all_location_data.shape[1], 4)
        self.assertEqual(first_data["locationId"], "23322232")
        self.assertEqual(first_data["name"], "La Polleria Alicante")
        self.assertEqual(
            first_data["description"],
            "We are revolutionizing Alicante! Come and try the waffle that is on everyone's mouth. Come and get your #pollofre or #pollolo, we are waiting for you!",
        )
        self.assertEqual(
            first_data["web_url"],
            "https://www.tripadvisor.com/Restaurant_Review-g1064230-d23322232-Reviews-La_Polleria_Alicante-Alicante_Costa_Blanca_Province_of_Alicante_Valencian_Commu.html?m=66827",
        )
