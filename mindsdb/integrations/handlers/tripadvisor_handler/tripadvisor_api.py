import requests
from requests import Response
from enum import Enum


class TripAdvisorAPICall(Enum):
    SEARCH_LOCATION = 1
    LOCATION_DETAILS = 2
    PHOTOS = 3


class TripAdvisorAPI:
    """A class for checking the connection to the TripAdvisor Content API and making requests.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
    """

    def __init__(self, api_key):
        self.api_key = api_key

    def connectTripAdvisor(self):
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

    def processQuery(self, url: str, params_dict: dict) -> str:
        """
        Processing the query and adding parameters to the URL
        """
        for idx, (queryParam, value) in enumerate(params_dict.items()):
            if value != None or value != "":
                if value != "" and any(
                    next_value != "" or value != None
                    for next_value in list(params_dict.values())[idx + 1 :]
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

        url = self.processQuery(url, params_dict)
        response = self.getResponse(url)

        return response

    def makeRequest(self, apiCall, **params):
        """
        Making a request based on the query
        """
        url = "https://api.content.tripadvisor.com/api/v1/location/"
        params_dict = params

        if apiCall == TripAdvisorAPICall.SEARCH_LOCATION:
            response = self.location_search(url, params_dict)

        return response.json()["data"]
