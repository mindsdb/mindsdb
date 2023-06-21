import requests


class TripAdvisorAPI:
    """A class for checking the connection to the TripAdvisor Content API.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
    """

    def __init__(self, api_key):
        self.api_key = api_key

    def connectTripAdvisor(self):
        url = "https://api.content.tripadvisor.com/api/v1/location/search?key={api_key}&searchQuery={searchQuery}&language=en".format(
            api_key=self.api_key, searchQuery="London"
        )

        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        status_code = response.status_code

        if status_code >= 400 and status_code <= 499:
            raise Exception("Client error: " + response.text)

        if status_code >= 500 and status_code <= 599:
            raise Exception("Server error: " + response.text)
