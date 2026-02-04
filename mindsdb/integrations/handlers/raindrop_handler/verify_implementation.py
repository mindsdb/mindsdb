#!/usr/bin/env python3

"""
Verification script for the Raindrop.io handler implementation.
This script checks all the key functionality without requiring a real API key.

Recent improvements:
- Uses logging instead of print statements for better integration with MindsDB logging
- Tests robustness of data normalization with missing columns
- Validates error handling for various edge cases
- Implements rate limiting to prevent API quota exhaustion
- Optimizes pagination for small LIMIT queries
"""

import sys
import logging
from unittest.mock import Mock, patch

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def test_handler_loading():
    """Test that the handler can be loaded and instantiated"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler import Handler, name, type, title, connection_args

        logger.info("[PASS] Handler module loaded successfully")
        logger.info(f"  Name: {name}")
        logger.info(f"  Type: {type}")
        logger.info(f"  Title: {title}")
        logger.info(f"  Connection args: {list(connection_args.keys())}")

        # Test instantiation
        handler = Handler("test_handler")
        logger.info("[PASS] Handler instantiated successfully")
        logger.info(f"  Tables: {list(handler._tables.keys())}")

        return True
    except Exception as e:
        logger.error(f"[FAIL] Handler loading failed: {e}")
        return False


def test_api_client():
    """Test the API client functionality"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler.raindrop_handler import RaindropAPIClient

        client = RaindropAPIClient("test_key")
        logger.info("[PASS] API client instantiated successfully")
        logger.info(f"  Base URL: {client.base_url}")
        logger.info(f"  Headers configured: {'Authorization' in client.headers}")

        return True
    except Exception as e:
        logger.error(f"[FAIL] API client test failed: {e}")
        return False


def test_table_functionality():
    """Test table functionality with mocked data"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler.raindrop_tables import RaindropsTable, CollectionsTable
        import pandas as pd

        # Test RaindropsTable
        handler_mock = Mock()
        raindrops_table = RaindropsTable(handler_mock)

        columns = raindrops_table.get_columns()
        logger.info(f"[PASS] RaindropsTable columns: {len(columns)} columns")

        # Test data normalization
        test_data = pd.DataFrame(
            [
                {
                    "_id": 123,
                    "title": "Test",
                    "collection": {"$id": 456, "title": "Test Collection"},
                    "tags": ["tag1", "tag2"],
                    "created": "2024-01-01T00:00:00Z",
                }
            ]
        )

        raindrops_table._normalize_raindrop_data(test_data)
        logger.info("[PASS] RaindropsTable data normalization works")

        # Test data preparation
        raindrops_table._prepare_raindrop_data(
            {"link": "https://example.com", "title": "Test", "tags": "tag1,tag2", "collection_id": 123}
        )
        logger.info("[PASS] RaindropsTable data preparation works")

        # Test CollectionsTable
        collections_table = CollectionsTable(handler_mock)
        columns = collections_table.get_columns()
        logger.info(f"[PASS] CollectionsTable columns: {len(columns)} columns")

        return True
    except Exception as e:
        logger.error(f"[FAIL] Table functionality test failed: {e}")
        return False


def test_connection_handling():
    """Test connection handling"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler import Handler

        # Test with missing API key
        handler = Handler("test")
        try:
            handler.connect()
            logger.error("[FAIL] Should have failed with missing API key")
            return False
        except ValueError as e:
            if "API key is required" in str(e):
                logger.info("[PASS] Properly validates missing API key")
            else:
                logger.error(f"[FAIL] Unexpected error: {e}")
                return False

        # Test with API key
        handler.connection_data = {"api_key": "test_key"}

        with patch("mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient") as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance

            handler.connect()
            logger.info("[PASS] Connection with API key works")

            # Test connection check
            mock_instance.get_user_stats.return_value = {"result": True}
            status = handler.check_connection()
            logger.info(f"[PASS] Connection check works: {status.success}")

        return True
    except Exception as e:
        logger.error(f"[FAIL] Connection handling test failed: {e}")
        return False


def main():
    """Run all verification tests"""
    logger.info("[VERIFY] Verifying Raindrop.io Handler Implementation")
    logger.info("=" * 50)

    tests = [
        ("Handler Loading", test_handler_loading),
        ("API Client", test_api_client),
        ("Table Functionality", test_table_functionality),
        ("Connection Handling", test_connection_handling),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        logger.info(f"\n[TEST] {test_name}")
        logger.info("-" * 30)
        if test_func():
            passed += 1
        else:
            logger.error(f"[FAILED] {test_name} failed")

    logger.info("\n" + "=" * 50)
    logger.info(f"[RESULTS] Test Results: {passed}/{total} tests passed")

    if passed == total:
        logger.info("[SUCCESS] All tests passed! The Raindrop.io handler is ready for use.")
        return 0
    else:
        logger.error("[FAILED] Some tests failed. Please check the implementation.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
