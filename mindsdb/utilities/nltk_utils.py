"""
NLTK Resource Management Utilities

Ensures required NLTK resources are available at runtime.
Provides graceful fallback with automatic downloading.
"""

import os
import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)

# Required NLTK resources for MindsDB operations
REQUIRED_NLTK_RESOURCES = {
    'tokenizers/punkt': 'punkt',
    'tokenizers/punkt_tab': 'punkt_tab',
    'corpora/stopwords': 'stopwords',
}


def check_nltk_resource(resource_path: str) -> bool:
    """
    Check if an NLTK resource is available.
    
    Args:
        resource_path: Path to the resource (e.g., 'tokenizers/punkt_tab')
        
    Returns:
        True if resource is available, False otherwise
    """
    try:
        import nltk
        nltk.data.find(resource_path)
        return True
    except (LookupError, ImportError):
        return False


def download_nltk_resource(resource_name: str, quiet: bool = True) -> bool:
    """
    Download an NLTK resource.
    
    Args:
        resource_name: Name of the resource to download (e.g., 'punkt_tab')
        quiet: Whether to suppress download progress output
        
    Returns:
        True if download succeeded, False otherwise
    """
    try:
        import nltk
        logger.info(f"Downloading NLTK resource: {resource_name}")
        nltk.download(resource_name, quiet=quiet)
        logger.info(f"Successfully downloaded NLTK resource: {resource_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to download NLTK resource '{resource_name}': {e}")
        return False


def ensure_nltk_resources(auto_download: bool = True) -> Dict[str, bool]:
    """
    Ensure all required NLTK resources are available.
    
    Args:
        auto_download: Whether to automatically download missing resources
        
    Returns:
        Dictionary mapping resource names to availability status
    """
    results = {}
    missing_resources = []
    
    for resource_path, resource_name in REQUIRED_NLTK_RESOURCES.items():
        available = check_nltk_resource(resource_path)
        results[resource_name] = available
        
        if not available:
            missing_resources.append((resource_path, resource_name))
            logger.warning(f"NLTK resource not found: {resource_path}")
    
    # Attempt to download missing resources if auto_download is enabled
    if auto_download and missing_resources:
        logger.info(f"Attempting to download {len(missing_resources)} missing NLTK resources")
        
        for resource_path, resource_name in missing_resources:
            success = download_nltk_resource(resource_name)
            results[resource_name] = success
            
            if not success:
                logger.error(
                    f"Could not download NLTK resource '{resource_name}'. "
                    f"Some NLP features may not work correctly. "
                    f"Manual download: python -m nltk.downloader {resource_name}"
                )
    
    elif missing_resources:
        logger.warning(
            f"Missing NLTK resources detected but auto_download is disabled. "
            f"Missing: {[name for _, name in missing_resources]}"
        )
    
    # Log summary
    available_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    logger.info(f"NLTK resources: {available_count}/{total_count} available")
    
    return results


def get_nltk_data_paths() -> List[str]:
    """
    Get all NLTK data search paths.
    
    Returns:
        List of directory paths where NLTK searches for data
    """
    try:
        import nltk
        return nltk.data.path
    except ImportError:
        return []


def initialize_nltk(auto_download: bool = None) -> None:
    """
    Initialize NLTK resources for MindsDB.
    
    This function should be called during MindsDB startup to ensure
    all required NLTK resources are available.
    
    Args:
        auto_download: Whether to auto-download missing resources.
                      If None, reads from environment variable MINDSDB_NLTK_AUTO_DOWNLOAD
                      (defaults to True in Docker, False otherwise)
    """
    try:
        import nltk
    except ImportError:
        logger.debug("NLTK not installed, skipping NLTK initialization")
        return
    
    # Determine auto_download setting
    if auto_download is None:
        # Default to True if running in Docker
        is_docker = os.path.exists('/.dockerenv') or os.getenv('MINDSDB_DOCKER_ENV') == '1'
        env_setting = os.getenv('MINDSDB_NLTK_AUTO_DOWNLOAD', 'true' if is_docker else 'false')
        auto_download = env_setting.lower() in ('true', '1', 'yes')
    
    logger.info("Initializing NLTK resources...")
    logger.debug(f"NLTK data paths: {get_nltk_data_paths()}")
    logger.debug(f"Auto-download enabled: {auto_download}")
    
    results = ensure_nltk_resources(auto_download=auto_download)
    
    # Check for critical failures
    critical_resources = ['punkt_tab', 'punkt']
    missing_critical = [r for r in critical_resources if not results.get(r, False)]
    
    if missing_critical:
        logger.warning(
            f"Critical NLTK resources missing: {missing_critical}. "
            f"NLP features may not work correctly."
        )
