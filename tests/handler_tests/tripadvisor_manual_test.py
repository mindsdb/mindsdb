from mindsdb.integrations.handlers.tripadvisor_handler.tripadvisor_handler import (
    TripAdvisorHandler,
)

"""
This .py script serves for manual testing, to see the responses in raw
JSON format.
"""

if __name__ == "__main__":
    handler = TripAdvisorHandler(
        connection_data={"api_key": "INSERT YOUR API KEY HERE"}
    )
    check_connection = handler.check_connection()

    # Testing SearchLocationTable
    result_searchLocation = handler.call_tripadvisor_searchlocation_api(
        params={"searchQuery": "london"}
    )

    # Testing LocationDetails
    result_locationDetails = handler.call_tripadvisor_location_details_api(
        params={"locationId": "23322232"}
    )

    print(result_searchLocation)
    print(result_locationDetails)
    print(handler.is_connected)
