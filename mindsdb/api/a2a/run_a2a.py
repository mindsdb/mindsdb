#!/usr/bin/env python
import os
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        logger.info("Successfully imported a2a module")
        
        # Run the main function, passing through command-line arguments
        a2a_main()
        
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