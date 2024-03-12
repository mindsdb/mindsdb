from prometheus_client import Summary


INTEGRATION_HANDLER_QUERY_TIME = Summary(
    'mindsdb_integration_handler_query_seconds',
    'How long integration handlers take to answer queries',
    ('integration', 'response_type')
)


INTEGRATION_HANDLER_RESPONSE_SIZE = Summary(
    'mindsdb_integration_handler_response_size',
    'How many rows are returned by an integration handler query',
    ('integration', 'response_type')
)
