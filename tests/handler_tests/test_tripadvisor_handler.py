from mindsdb.integrations.handlers.tripadvisor_handler.tripadvisor_handler import (
    TripAdvisorHandler,
)

from mindsdb.integrations.handlers.tripadvisor_handler.tripadvisor_table import (
    SearchLocationTable,
    LocationDetailsTable,
    ReviewsTable,
    PhotosTable,
    NearbyLocationTable,
)

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

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
            where='searchQuery = "London"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

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
            where='searchQuery = "23322232"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

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


class ReviewTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        trades_table = ReviewsTable(api_handler)
        # Order matters.
        expected_columns = [
            "id",
            "lang",
            "location_id",
            "published_date",
            "rating",
            "helpful_votes",
            "rating_image_url",
            "url",
            "trip_type",
            "travel_date",
            "text_review",
            "title",
            "owner_response",
            "is_machine_translated",
            "user",
            "subratings",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        api_handler.call_tripadvisor_searchlocation_api.return_value = pd.DataFrame(
            [
                [
                    "921095426",  # id
                    "en",  # lang
                    "99288",  # location_id
                    "2023-10-13T09:48:46Z",  # published_date
                    "1",  # rating
                    "Check in is at 4:00 - my room wasn't ready until 9:00.  I had no A/C  on my first night and it was too late for engineering to fix it.  I was offered a $40 discount from some ridiculous hoteL fee - that was a joke on a 700/night stay.  I would not recommend this place at all.",  # text review
                    "POOR CHECK -IN  - NO A/C AND NO DISCOUNT",  # title
                ]
            ]
        )
        tripadvisor_table = ReviewsTable(api_handler)

        id_identifier = Identifier(path_str='id')
        lang_identifier = Identifier(path_str='en')
        location_id_identifier = Identifier(path_str='location_id')
        published_date_identifier = Identifier(path_str='published_date')
        rating_identifier = Identifier(path_str='rating')
        text_review_identifier = Identifier(path_str='room_id')
        title_identifier = Identifier(path_str='text')
        select_all = ast.Select(
            targets=[id_identifier, lang_identifier, location_id_identifier, published_date_identifier, rating_identifier, text_review_identifier, title_identifier],
            from_table="reviewsTable",
            where='locationId = "99288"',
        )

        review_data = tripadvisor_table.select(select_all)
        first_data = review_data.iloc[0]

        self.assertEqual(review_data.shape[1], 7)
        self.assertEqual(first_data["id"], "921095426")
        self.assertEqual(first_data["en"], "en")
        self.assertEqual(first_data["rating"], "1")
        self.assertEqual(first_data["title"], "POOR CHECK -IN  - NO A/C AND NO DISCOUNT")


class PhotosTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        trades_table = PhotosTable(api_handler)
        # Order matters.
        expected_columns = [
            "id",
            "is_blessed",
            "album",
            "caption",
            "published_date",
            "images",
            "source",
            "user",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        api_handler.call_tripadvisor_searchlocation_api.return_value = pd.DataFrame(
            [
                [
                    "673312657",  # id
                    "false",  # is_blessed
                    "Hotel & Grounds",  # album
                    "{'name': 'Management', 'localized_name': 'Management'}",  # source
                ]
            ]
        )
        tripadvisor_table = PhotosTable(api_handler)

        id_identifier = Identifier(path_str='id')
        is_blessed_identifier = Identifier(path_str='is_blessed')
        album_identifier = Identifier(path_str='album')
        source_identifier = Identifier(path_str='source')
        select_all = ast.Select(
            targets=[id_identifier, is_blessed_identifier, album_identifier, source_identifier],
            from_table="photosTable",
            where='locationId = "99288"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

        self.assertEqual(all_location_data.shape[1], 4)
        self.assertEqual(first_data["id"], "673312657")
        self.assertEqual(first_data["is_blessed"], "false")
        self.assertEqual(first_data["album"], "Hotel & Grounds")
        self.assertEqual(first_data["source"], "{'name': 'Management', 'localized_name': 'Management'}")


class NearbySearchTableTest(unittest.TestCase):
    def test_get_columns_returns_all_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        trades_table = NearbyLocationTable(api_handler)
        # Order matters.
        expected_columns = [
            "location_id",
            "name",
            "distance",
            "rating",
            "bearing",
            "address_obj",
        ]
        self.assertListEqual(trades_table.get_columns(), expected_columns)

    def test_select_returns_some_columns(self):
        api_handler = Mock(TripAdvisorHandler)
        api_handler.call_tripadvisor_searchlocation_api.return_value = pd.DataFrame(
            [
                [
                    "210108",  # location_id
                    "American Museum of Natural History",  # name
                    "0.039615104835680856",  # distance
                    "{'street1': '79th Street', 'street2': 'Central Park West', 'city': 'New York City', 'state': 'New York', 'country': 'United States', 'postalcode': '10024', 'address_string': '79th Street Central Park West, New York City, NY 10024'}",  # address_obj
                ]
            ]
        )
        tripadvisor_table = NearbyLocationTable(api_handler)

        location_id_identifier = Identifier(path_str='location_id')
        name_identifier = Identifier(path_str='name')
        distance_identifier = Identifier(path_str='distance')
        address_obj_identifier = Identifier(path_str='address_obj')
        select_all = ast.Select(
            targets=[location_id_identifier, name_identifier, distance_identifier, address_obj_identifier],
            from_table="nearbyLocationTable",
            where='latLong = "40.780825, -73.972781"',
        )

        all_location_data = tripadvisor_table.select(select_all)
        first_data = all_location_data.iloc[0]

        self.assertEqual(all_location_data.shape[1], 4)
        self.assertEqual(first_data["location_id"], "210108")
        self.assertEqual(first_data["name"], "American Museum of Natural History")
        self.assertEqual(first_data["distance"], "0.039615104835680856")
        self.assertEqual(first_data["address_obj"], "{'street1': '79th Street', 'street2': 'Central Park West', 'city': 'New York City', 'state': 'New York', 'country': 'United States', 'postalcode': '10024', 'address_string': '79th Street Central Park West, New York City, NY 10024'}")
