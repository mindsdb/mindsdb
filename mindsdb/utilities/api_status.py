import os
import json

from mindsdb.utilities.config import config
from mindsdb.utilities import log


logger = log.getLogger(__name__)
_api_status_file = None


def _get_api_status_file():
    global _api_status_file
    if _api_status_file is None:
        # Use a temporary file that can be shared across processes.
        temp_dir = config["paths"]["tmp"]
        _api_status_file = os.path.join(temp_dir, "mindsdb_api_status.json")
        # Overwrite the file if it exists.
        if os.path.exists(_api_status_file):
            try:
                os.remove(_api_status_file)
            except OSError:
                logger.exception(f"Error removing existing API status file: {_api_status_file}")

    return _api_status_file


def get_api_status():
    """Get the current API status from the shared file."""
    status_file = _get_api_status_file()
    try:
        if os.path.exists(status_file):
            with open(status_file, "r") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {}


def set_api_status(api_name: str, status: bool):
    """Set the status of an API in the shared file."""
    status_file = _get_api_status_file()
    current_status = get_api_status()
    current_status[api_name] = status

    # Write atomically to avoid race conditions.
    temp_file = status_file + ".tmp"
    try:
        with open(temp_file, "w") as f:
            json.dump(current_status, f)
        os.replace(temp_file, status_file)
    except IOError:
        # Clean up temp file if it exists.
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except OSError:
                pass
