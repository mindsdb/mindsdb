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
    """Add the a2a directory to Python path for importing modules"""
    # Get the directory containing this script (should be mindsdb root)
    mindsdb_root = os.path.dirname(os.path.abspath(__file__))
    
    # Add the a2a directory to Python path
    a2a_dir = os.path.join(mindsdb_root, 'mindsdb', 'api', 'a2a')
    sys.path.insert(0, a2a_dir)
    
    logger.info(f"Added {a2a_dir} to Python path")

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