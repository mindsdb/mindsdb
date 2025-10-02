import os
import pytest
import requests
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
load_dotenv()


def pytest_addoption(parser):
    """Adds the --api-url command-line option to pytest."""
    parser.addoption(
        "--api-url",
        action="store",
        default=os.getenv("MINDSDB_API_URL", "http://127.0.0.1:47334"),
        help="Base URL for the MindsDB API.",
    )


@pytest.fixture(scope="session")
def api_url(request):
    """
    Fixture to provide the base URL for the API.
    It can be set via the --api-url command-line option or the MINDSDB_API_URL environment variable.
    """
    return request.config.getoption("--api-url")


@pytest.fixture(scope="session")
def api_client(api_url):
    """
    A session-scoped requests client for making API calls.
    Using a session object is more efficient for multiple requests.
    """
    session = requests.Session()
    # We add a custom `base_url` attribute to the session object for convenience.
    session.base_url = api_url
    return session
