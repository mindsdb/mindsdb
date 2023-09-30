from tripadvisor_handler import TripAdvisorHandler

"""
This .py script serves for manual testing
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

    result_locationDetails = handler.call_tripadvisor_searchlocation_api(
        params={"locationId": "23322232"}
    )

    print(result_searchLocation)
    print(result_locationDetails)
    print(handler.is_connected)
