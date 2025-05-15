#!/usr/bin/env python
import os
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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

    logger.info("Added %s to PYTHONPATH", repo_root)


def main():
    """
    Run the a2a module with the correct Python path.
    First set up the Python path, then import and run __main__.py
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

        # Get configuration from config system
        a2a_config = config.get("a2a", {})

        # Prepare command line arguments based on configuration
        sys_argv = sys.argv[:]  # Make a copy of the original argv

        # Only add args that aren't already in sys.argv
        if "--host" not in " ".join(sys_argv) and a2a_config.get("host"):
            sys_argv.extend(["--host", a2a_config["host"]])

        if "--port" not in " ".join(sys_argv) and a2a_config.get("port"):
            sys_argv.extend(["--port", str(a2a_config["port"])])

        if "--mindsdb-host" not in " ".join(sys_argv) and a2a_config.get(
            "mindsdb_host"
        ):
            sys_argv.extend(["--mindsdb-host", a2a_config["mindsdb_host"]])

        if "--mindsdb-port" not in " ".join(sys_argv) and a2a_config.get(
            "mindsdb_port"
        ):
            sys_argv.extend(["--mindsdb-port", str(a2a_config["mindsdb_port"])])

        if "--agent-name" not in " ".join(sys_argv) and a2a_config.get("agent_name"):
            sys_argv.extend(["--agent-name", a2a_config["agent_name"]])

        if "--project-name" not in " ".join(sys_argv) and a2a_config.get(
            "project_name"
        ):
            sys_argv.extend(["--project-name", a2a_config["project_name"]])

        # Temporarily replace sys.argv with our constructed arguments
        original_argv = sys.argv
        sys.argv = sys_argv

        # Run the main function with the configured arguments
        a2a_main()

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
