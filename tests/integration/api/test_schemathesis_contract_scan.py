import os
import pytest
import schemathesis
from hypothesis import settings

# Get the OpenAPI spec URL from an environment variable, defaulting to the correct local endpoint.
MINDS_OPENAPI_SPEC_URL = os.getenv(
    "MINDS_OPENAPI_SPEC_URL", "http://127.0.0.1:47334/api/swagger.json"
)

try:
    # This is the correct loading method for your schemathesis version.
    # It requires the test server to be running so it can fetch the URL.
    schema = schemathesis.openapi.from_url(MINDS_OPENAPI_SPEC_URL)
except Exception as e:
    pytest.fail(
        f"Failed to load OpenAPI schema from {MINDS_OPENAPI_SPEC_URL}. "
        f"This test requires the MindsDB server to be running. Error: {e}"
    )


@schema.parametrize()
@settings(deadline=2000)
def test_api_contract_scan(case, client):
    """
    Runs Schemathesis contract scans against all API endpoints.
    """
    # CRITICAL CHANGE: Use `case.call()` and pass the `client` as the session.
    # This resolves the `AttributeError` by using the method that corresponds
    # to loading from a URL, and it directs the requests to the in-memory
    # test application instead of the live network.
    response = case.call(session=client)

    # Validate the response against the schema's expectations.
    case.validate_response(response)