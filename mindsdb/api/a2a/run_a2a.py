#!/usr/bin/env python
import os
import sys
import logging
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_python_path():
    """Ensure the repository root directory is on *sys.path* so that the
    ``mindsdb`` package (located at <repo>/mindsdb) can be imported when this
    helper script is executed from inside *mindsdb/api/a2a*.
    """

    # Absolute path to *this* file
    this_file_dir = os.path.dirname(os.path.abspath(__file__))

    # Walk three levels up:  a2a/ -> api/ -> mindsdb/ -> <repo_root>
    repo_root = os.path.abspath(
        os.path.join(this_file_dir, os.pardir, os.pardir, os.pardir)
    )

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
        # Import the main function from __main__.py
        from mindsdb.api.a2a.__main__ import main as a2a_main
        from mindsdb.utilities.config import config

        logger.info("Successfully imported a2a module")

        # Get configuration from config system or use provided override
        a2a_config = config_override if config_override is not None else config.get('a2a', {})

        # Set log level if specified
        if a2a_config.get('log_level'):
            log_level = getattr(logging, a2a_config['log_level'].upper(), None)
            if log_level:
                logger.setLevel(log_level)
                logger.info(f"Set log level to {a2a_config['log_level'].upper()}")

        # Prepare command line arguments based on configuration
        sys_argv = []  # Start with an empty list instead of copying sys.argv

        # Only add args that aren't already in sys.argv
        if a2a_config.get('host'):
            sys_argv.extend(['--host', a2a_config['host']])

        if a2a_config.get('port'):
            sys_argv.extend(['--port', str(a2a_config['port'])])

        if a2a_config.get('mindsdb_host'):
            sys_argv.extend(['--mindsdb-host', a2a_config['mindsdb_host']])

        if a2a_config.get('mindsdb_port'):
            sys_argv.extend(['--mindsdb-port', str(a2a_config['mindsdb_port'])])

        if a2a_config.get('project_name'):
            sys_argv.extend(['--project-name', a2a_config['project_name']])

        if a2a_config.get('log_level'):
            sys_argv.extend(['--log-level', a2a_config['log_level']])

        logger.info(f"Starting A2A with arguments: {' '.join(sys_argv)}")

        # Temporarily replace sys.argv with our constructed arguments
        original_argv = sys.argv
        sys.argv = [sys.argv[0]] + sys_argv

        # Run the main function with the configured arguments
        a2a_args = []
        for arg in sys_argv:
            # Skip the --api argument and its value
            if arg.startswith('--api=') or arg == '--api':
                continue
            # Skip the value of --api if the previous arg was --api
            if arg.startswith('--') and sys_argv[sys_argv.index(arg) - 1] == '--api':
                continue
            a2a_args.append(arg)

        a2a_main(a2a_args)

        # Restore original sys.argv
        sys.argv = original_argv

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
