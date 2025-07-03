#!/usr/bin/env python
import os
import sys
import logging
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def setup_python_path():
    """Ensure the repository root directory is on *sys.path* so that the
    ``mindsdb`` package (located at <repo>/mindsdb) can be imported when this
    helper script is executed from inside *mindsdb/api/a2a*.
    """

    # Absolute path to *this* file
    this_file_dir = os.path.dirname(os.path.abspath(__file__))

    # Walk three levels up:  a2a/ -> api/ -> mindsdb/ -> <repo_root>
    repo_root = os.path.abspath(os.path.join(this_file_dir, os.pardir, os.pardir, os.pardir))

    # Prepend to PYTHONPATH if not already present
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    # Add the a2a directory to the Python path so that 'common' can be imported
    if this_file_dir not in sys.path:
        sys.path.insert(0, this_file_dir)

    logger.info("Added %s to PYTHONPATH", repo_root)


def main(config_override: Optional[Dict[str, Any]] = None, *args, **kwargs):
    """
    Run the a2a module with the correct Python path.
    First set up the Python path, then import and run __main__.py

    Args:
        config_override: Optional configuration dictionary to override settings
        args: Additional positional arguments
        kwargs: Additional keyword arguments
    """
    # Set up Python path first
    setup_python_path()

    # Load environment variables
    load_dotenv()

    try:
        # Import the main_with_config function from __main__.py
        from mindsdb.api.a2a.__main__ import main_with_config
        from mindsdb.utilities.config import config

        logger.info("Successfully imported a2a module")

        # Get configuration from config system or use provided override
        a2a_config = config_override if config_override is not None else config.get("api", {}).get("a2a", {})

        # Set log level if specified
        if a2a_config.get("log_level"):
            log_level = getattr(logging, a2a_config["log_level"].upper(), None)
            if log_level:
                logger.setLevel(log_level)
                logger.info(f"Set log level to {a2a_config['log_level'].upper()}")

        logger.info(f"Starting A2A with configuration: {a2a_config}")

        # Call the main_with_config function with the configuration
        main_with_config(a2a_config, *args, **kwargs)

    except ImportError as e:
        logger.error(f"Error importing a2a module: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running a2a module: {e}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
