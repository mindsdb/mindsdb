import os
from dotenv import load_dotenv
from tests.unit.api.http.conftest import app, client  # noqa F401

# Load environment variables from .env file
load_dotenv()