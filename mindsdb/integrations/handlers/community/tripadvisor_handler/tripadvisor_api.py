import requests
from requests import Response
from enum import Enum


class TripAdvisorAPICall(Enum):
    """TripAdvisor API references"""

    SEARCH_LOCATION = 1
    LOCATION_DETAILS = 2
    PHOTOS = 3
    REVIEWS = 4
    NEARBY_SEARCH = 5


class TripAdvisorAPI:
    """A class for checking the connection to the TripAdvisor Content API and making requests.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
    """

    def __init__(self, api_key):
        self.api_key = api_key

    def checkTripAdvisorConnection(self):
        """
        Check the connection with TripAdvisor
        """
        url = "https://api.content.tripadvisor.com/api/v1/location/search?language=en&key={api_key}&searchQuery={searchQuery}".format(
            api_key=self.api_key, searchQuery="London"
        )

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        status_code = response.status_code

        if status_code >= 400 and status_code <= 499:
            raise Exception("Client error: " + response.text)

        if status_code >= 500 and status_code <= 599:
            raise Exception("Server error: " + response.text)

    def getResponse(self, url: str) -> Response:
        """
        Getting a response from the API call
        """
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        return response

    def getURLQuery(self, url: str, params_dict: dict) -> str:
        """
        Processing the query and adding parameters to the URL
        """
        for idx, (queryParam, value) in enumerate(params_dict.items()):
            if value is not None or value != "":
                if value != "" and any(
                    next_value != "" or value is not None
                    for next_value in list(params_dict.values())[idx + 1:]
                ):
                    url += "{queryParam}={value}&".format(
                        queryParam=queryParam, value=value
                    )
                else:
                    url += "{queryParam}={value}".format(
                        queryParam=queryParam, value=value
                    )
        return url

    def location_search(
        self,
        url: str,
        params_dict: dict,
        language: str = "en",
    ) -> Response:
        """
        The Location Search request returns up to 10 locations found by the given search query. You can use category ("hotels", "attractions", "restaurants", "geos"),
        phone number, address, and latitude/longtitude to search with more accuracy.

        Args:
            searchQuery (str): Text to use for searching based on the name of the location.
            category (str): Filters result set based on property type. Valid options are "hotels", "attractions", "restaurants", and "geos".
            phone (str): Phone number to filter the search results by (this can be in any format with spaces and dashes but without the "+" sign at the beginning).
            address (str): Address to filter the search results by.
            latLong (str): Latitude/Longitude pair to scope down the search around a specifc point - eg. "42.3455,-71.10767".
            radius (int): Length of the radius from the provided latitude/longitude pair to filter results.
            radiusUnit (str): Unit for length of the radius. Valid options are "km", "mi", "m" (km=kilometers, mi=miles, m=meters.
            language (str): The language in which to return results (e.g. "en" for English or "es" for Spanish) from the list of our Supported Languages.

        Returns:
            response: Response object with response data as application/json
        """

        url = url + "search?language={language}&key={api_key}&".format(
            api_key=self.api_key, language=language
        )

        url = self.getURLQuery(url, params_dict)
        response = self.getResponse(url)

        return response

    def location_details(
        self, url: str, params_dict: dict, locationId: str, language: str = "en"
    ) -> Response:
        """
        A Location Details request returns comprehensive information about a location (hotel, restaurant, or an attraction) such as name, address, rating, and URLs for the listing
        on Tripadvisor.

        Args:
            locationId (str): A unique identifier for a location on Tripadvisor. The location ID can be obtained using the Location Search.
            language (str): The language in which to return results (e.g. "en" for English or "es" for Spanish) from the list of our Supported Languages.
            currency (str): The currency code to use for request and response (should follow ISO 4217).

        Returns:
            response (Response): Response object with response data as application/json
        """
        url = url + "{locationId}/details?language={language}&key={api_key}&".format(
            locationId=locationId, api_key=self.api_key, language=language
        )
        url = self.getURLQuery(url, params_dict)
        response = self.getResponse(url)
        return response

    def location_reviews(
        self, url: str, locationId: str, language: str = "en"
    ) -> Response:
        """
        The Location Reviews request returns up to 5 of the most recent reviews for a specific location. Please note that the limits are different for the beta subscribers.

        Args:
            locationId (str): A unique identifier for a location on Tripadvisor. The location ID can be obtained using the Location Search.
            language (str): The language in which to return results (e.g. "en" for English or "es" for Spanish) from the list of our Supported Languages.

        Returns:
            response: Response object with response data as application/json
        """
        url = url + "{locationId}/reviews?language={language}&key={api_key}&".format(
            locationId=locationId, api_key=self.api_key, language=language
        )

        response = self.getResponse(url)
        return response

    def location_photos(self, url: str, locationId: str, language: str = "en") -> Response:
        """
        The Location Photos request returns up to 5 high-quality photos for a specific location. Please note that the limits are different for the beta subscribers.
        You need to upgrade to get the higher limits mentioned here. The photos are ordered by recency.

        Args:
            locationId (str): A unique identifier for a location on Tripadvisor. The location ID can be obtained using the Location Search.
            language (str): The language in which to return results (e.g. "en" for English or "es" for Spanish) from the list of our Supported Languages.

        Returns:
            response: Response object with response data as application/json
        """
        url = url + "{locationId}/photos?language={language}&key={api_key}".format(locationId=locationId, language=language, api_key=self.api_key)
        response = self.getResponse(url)
        return response

    def location_nearby_search(self, url: str, params_dict: dict, language: str = "en") -> Response:
        """
        The Nearby Location Search request returns up to 10 locations found near the given latitude/longtitude.
        You can use category ("hotels", "attractions", "restaurants", "geos"), phone number, address to search with more accuracy.

        Args:
            latLong (str): Latitude/Longitude pair to scope down the search around a specifc point - eg. "42.3455,-71.10767".
            category (str): Filters result set based on property type. Valid options are "hotels", "attractions", "restaurants", and "geos".
            phone (str): Phone number to filter the search results by (this can be in any format with spaces and dashes but without the "+" sign at the beginning).
            address (str): Address to filter the search results by.
            radius (int): Length of the radius from the provided latitude/longitude pair to filter results.
            radiusUnit (str): Unit for length of the radius. Valid options are "km", "mi", "m" (km=kilometers, mi=miles, m=meters.
            language (str): The language in which to return results (e.g. "en" for English or "es" for Spanish) from the list of our Supported Languages.

        Returns:
            response: Response object with response data as application/json
        """

        url = url + "nearby_search?language={language}&key={api_key}&".format(
            api_key=self.api_key, language=language
        )

        url = self.getURLQuery(url, params_dict)
        response = self.getResponse(url)
        return response

    def getTripAdvisorData(self, apiCall, **params):
        """
        Making a request based on the query and receive data from TripAdvisor.
        """
        url = "https://api.content.tripadvisor.com/api/v1/location/"
        params_dict = params

        if apiCall == TripAdvisorAPICall.SEARCH_LOCATION:
            response = self.location_search(url, params_dict)
            return response.json()["data"]

        elif apiCall == TripAdvisorAPICall.LOCATION_DETAILS:
            response = self.location_details(
                url, params_dict, params_dict["locationId"]
            )
            return response.json()

        elif apiCall == TripAdvisorAPICall.REVIEWS:
            response = self.location_reviews(url, params_dict["locationId"])
            return response.json()["data"]

        elif apiCall == TripAdvisorAPICall.PHOTOS:
            response = self.location_photos(url, params_dict["locationId"])
            return response.json()["data"]

        elif apiCall == TripAdvisorAPICall.NEARBY_SEARCH:
            response = self.location_nearby_search(url, params_dict)
            return response.json()["data"]
