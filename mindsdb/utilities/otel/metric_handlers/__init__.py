from mindsdb.utilities.otel.prepare import metrics, OTEL_SERVICE_NAME

_query_request_counter = None


def get_query_request_counter():
    """
    Get the query request counter
    """
    global _query_request_counter

    # Create the counter if it does not exist
    if _query_request_counter is None:
        meter_name = f"{OTEL_SERVICE_NAME}.query_service_meter"
        
        # Get the meter from the main metrics object
        meter = metrics.get_meter(meter_name)

        _query_request_counter = meter.create_counter(
            name="query_request_count",
            description="Counts the number of times the SQL query is called",
            unit="1",
        )

    return _query_request_counter

def increment_otel_query_request_counter(metadata: dict) -> None:
    query_request_counter = get_query_request_counter()
    query_request_counter.add(1, metadata)
