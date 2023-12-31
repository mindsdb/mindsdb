import os

import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.api_handler import APIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from .tripadvisor_table import SearchLocationTable
from .tripadvisor_table import LocationDetailsTable
from .tripadvisor_table import ReviewsTable
from .tripadvisor_table import PhotosTable
from .tripadvisor_table import NearbyLocationTable
from .tripadvisor_api import TripAdvisorAPI
from .tripadvisor_api import TripAdvisorAPICall

logger = log.getLogger(__name__)


class TripAdvisorHandler(APIHandler):
    """A class for handling connections and interactions with the TripAdvisor Content API.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
        api (TripAdvisorAPI): The `TripAdvisorAPI` object for checking the connection to the TripAdvisor API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self._tables = {}

        self.connection_args = {}
        handler_config = Config().get("tripadvisor_handler", {})
        for k in ["api_key"]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f"TRIPADVISOR_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"TRIPADVISOR_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        tripAdvisor = SearchLocationTable(self)
        self._register_table("searchLocationTable", tripAdvisor)

        tripAdvisorLocationDetails = LocationDetailsTable(self)
        self._register_table("locationDetailsTable", tripAdvisorLocationDetails)

        tripAdvisorReviews = ReviewsTable(self)
        self._register_table("reviewsTable", tripAdvisorReviews)

        tripAdvisorPhotos = PhotosTable(self)
        self._register_table("photosTable", tripAdvisorPhotos)

        tripAdvisorNearbyLocation = NearbyLocationTable(self)
        self._register_table("nearbyLocationTable", tripAdvisorNearbyLocation)

    def connect(self, api_version=2):
        """Check the connection with TripAdvisor API"""

        if self.is_connected is True:
            return self.api

        self.api = TripAdvisorAPI(api_key=self.connection_args["api_key"])

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        """This function evaluates if the connection is alive and healthy"""
        response = StatusResponse(False)

        try:
            api = self.connect()

            # make a random http call with searching a location.
            #   it raises an error in case if auth is not success and returns not-found otherwise
            api.connectTripAdvisor()
            response.success = True

        except Exception as e:
            response.error_message = f"Error connecting to TripAdvisor api: {e}"
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def call_tripadvisor_searchlocation_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        """It processes the JSON data from the call and transforms it into pandas.Dataframe"""
        if self.is_connected is False:
            self.connect()

        locations = self.api.getTripAdvisorData(
            TripAdvisorAPICall.SEARCH_LOCATION, **params
        )
        result = []

        for loc in locations:
            data = {
                "location_id": loc.get("location_id"),
                "name": loc.get("name"),
                "distance": loc.get("distance"),
                "rating": loc.get("rating"),
                "bearing": loc.get("bearing"),
                "street1": loc.get("address_obj").get("street1"),
                "street2": loc.get("address_obj").get("street2"),
                "city": loc.get("address_obj").get("city"),
                "state": loc.get("address_obj").get("state"),
                "country": loc.get("address_obj").get("country"),
                "postalcode": loc.get("address_obj").get("postalcode"),
                "address_string": loc.get("address_obj").get("address_string"),
                "phone": loc.get("address_obj").get("phone"),
                "latitude": loc.get("address_obj").get("latitude"),
                "longitude": loc.get("address_obj").get("longitude"),
            }
            result.append(data)
        result = pd.DataFrame(result)
        return result

    def call_tripadvisor_location_details_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        """It processes the JSON data from the call and transforms it into pandas.Dataframe"""
        if self.is_connected is False:
            self.connect()

        loc = self.api.getTripAdvisorData(TripAdvisorAPICall.LOCATION_DETAILS, **params)
        result = []

        data = {
            "location_id": loc.get("location_id"),
            "name": loc.get("name"),
            "distance": loc.get("distance"),
            "rating": loc.get("rating"),
            "bearing": loc.get("bearing"),
            "street1": loc.get("address_obj").get("street1"),
            "street2": loc.get("address_obj").get("street2"),
            "city": loc.get("address_obj").get("city"),
            "state": loc.get("address_obj").get("state"),
            "country": loc.get("address_obj").get("country"),
            "postalcode": loc.get("address_obj").get("postalcode"),
            "address_string": loc.get("address_obj").get("address_string"),
            "phone": loc.get("address_obj").get("phone"),
            "latitude": loc.get("address_obj").get("latitude"),
            "longitude": loc.get("address_obj").get("longitude"),
            "web_url": loc.get("web_url"),
            "timezone": loc.get("timezone"),
            "email": loc.get("email"),
            "website": loc.get("website"),
            "write_review": loc.get("write_review"),
            "ranking_data": str(loc.get("ranking_data")),
            "rating_image_url": loc.get("rating_image_url"),
            "num_reviews": loc.get("num_reviews"),
            "review_rating_count": loc.get("review_rating_count"),
            "subratings": loc.get("subratings"),
            "photo_count": loc.get("photo_count"),
            "see_all_photos": loc.get("see_all_photos"),
            "price_level": loc.get("price_level"),
            "parent_brand": loc.get("parent_brand"),
            "brand": loc.get("brand"),
            "ancestors": str(loc.get("ancestors")),
            "periods": str(loc.get("hours").get("periods"))
            if loc.get("hours") is not None
            else None,
            "weekday": str(loc.get("hours").get("weekday_text"))
            if loc.get("weekday") is not None
            else None,
            "amenities": str(loc.get("amenities")),
            "features": str(loc.get("features")),
            "cuisines": str(loc.get("cuisine")),
            "styles": str(loc.get("styles")),
            "neighborhood_info": str(loc.get("neighborhood_info")),
            "awards": str(loc.get("awards")),
            "trip_types": str(loc.get("trip_types")),
            "groups": str(loc.get("groups")),
        }

        result.append(data)

        result = pd.DataFrame(result)
        return result

    def call_tripadvisor_reviews_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        """It processes the JSON data from the call and transforms it into pandas.Dataframe"""
        if self.is_connected is False:
            self.connect()

        locations = self.api.getTripAdvisorData(TripAdvisorAPICall.REVIEWS, **params)
        result = []

        for loc in locations:
            data = {
                "id": loc.get("id"),
                "lang": loc.get("lang"),
                "location_id": loc.get("location_id"),
                "published_date": loc.get("published_date"),
                "rating": loc.get("rating"),
                "helpful_votes": loc.get("helpful_votes"),
                "rating_image_url": loc.get("rating_image_url"),
                "url": loc.get("url"),
                "trip_type": loc.get("trip_type"),
                "travel_date": loc.get("travel_date"),
                "text_review": loc.get("text"),
                "title": loc.get("title"),
                "owner_response": loc.get("owner_response"),
                "is_machine_translated": loc.get("is_machine_translated"),
                "user": str(loc.get("user")),
                "subratings": str(loc.get("subratings")),
            }
            result.append(data)
        result = pd.DataFrame(result)
        return result

    def call_tripadvisor_photos_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        """It processes the JSON data from the call and transforms it into pandas.Dataframe"""
        if self.is_connected is False:
            self.connect()

        locations = self.api.getTripAdvisorData(TripAdvisorAPICall.PHOTOS, **params)
        result = []

        for loc in locations:
            data = {
                "id": loc.get("id"),
                "is_blessed": loc.get("is_blessed"),
                "album": loc.get("album"),
                "caption": loc.get("caption"),
                "published_date": loc.get("published_date"),
                "images": str(loc.get("images")),
                "source": str(loc.get("source")),
                "user": str(loc.get("user")),
            }
            result.append(data)
        result = pd.DataFrame(result)
        return result

    def call_tripadvisor_nearby_location_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        """It processes the JSON data from the call and transforms it into pandas.Dataframe"""
        if self.is_connected is False:
            self.connect()

        locations = self.api.getTripAdvisorData(TripAdvisorAPICall.NEARBY_SEARCH, **params)
        result = []

        for loc in locations:
            data = {
                "location_id": loc.get("location_id"),
                "name": loc.get("name"),
                "distance": loc.get("distance"),
                "rating": loc.get("rating"),
                "bearing": loc.get("bearing"),
                "address_obj": str(loc.get("address_obj")),
            }
            result.append(data)
        result = pd.DataFrame(result)
        return result
