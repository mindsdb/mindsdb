from tripadvisor_handler import TripAdvisorHandler

if __name__ == "__main__":
    handler = TripAdvisorHandler(
        connection_data={"api_key": "F40A115DC7894CEAB894BE191ACD40F6"}
    )
    lol = handler.check_connection()
    result = handler.call_tripadvisor_searchlocation_api(
        params={"searchQuery": "london"}
    )
    print(result)
    print(handler.is_connected)
