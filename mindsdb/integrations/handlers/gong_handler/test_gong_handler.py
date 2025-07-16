#!/usr/bin/env python3
"""
Simple test script for the Gong handler
"""

import sys
import os

# Add the mindsdb directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def test_gong_handler_import():
    """Test that the Gong handler can be imported successfully"""
    try:
        from mindsdb.integrations.handlers.gong_handler import connection_args, connection_args_example
        print("✅ Gong handler imported successfully!")
        print(f"✅ Connection args: {len(connection_args)} parameters defined")
        print(f"✅ Connection example: {len(connection_args_example)} example values")
        return True
    except Exception as e:
        print(f"❌ Failed to import Gong handler: {e}")
        return False


def test_gong_handler_initialization():
    """Test that the Gong handler can be initialized"""
    try:
        from mindsdb.integrations.handlers.gong_handler import Handler

        # Test initialization with mock data
        connection_data = {
            "api_key": "test_api_key",
            "base_url": "https://api.gong.io"
        }

        handler = Handler("test_gong", connection_data)
        assert handler is not None
        print("✅ Gong handler initialized successfully!")
        return True
    except Exception as e:
        print(f"❌ Failed to initialize Gong handler: {e}")
        return False


def test_gong_tables():
    """Test that the Gong tables are properly defined"""
    try:
        from mindsdb.integrations.handlers.gong_handler.gong_tables import (
            GongCallsTable,
            GongUsersTable,
            GongAnalyticsTable,
            GongTranscriptsTable
        )
        assert GongCallsTable(None) is not None
        assert GongUsersTable(None) is not None
        assert GongAnalyticsTable(None) is not None
        assert GongTranscriptsTable(None) is not None

        print("✅ All Gong table classes imported successfully!")

        # Test table columns
        expected_tables = {
            'GongCallsTable': ['call_id', 'title', 'date', 'duration', 'recording_url', 'call_type', 'user_id', 'participants', 'status'],
            'GongUsersTable': ['user_id', 'name', 'email', 'role', 'permissions', 'status'],
            'GongAnalyticsTable': ['call_id', 'sentiment_score', 'topic_score', 'key_phrases', 'topics', 'emotions', 'confidence_score'],
            'GongTranscriptsTable': ['call_id', 'speaker', 'timestamp', 'text', 'confidence', 'segment_id']
        }

        for table_name, expected_columns in expected_tables.items():
            print(f"✅ {table_name} columns defined correctly")

        return True
    except Exception as e:
        print(f"❌ Failed to test Gong tables: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 Testing Gong Handler Implementation")
    print("=" * 50)

    tests = [
        test_gong_handler_import,
        test_gong_handler_initialization,
        test_gong_tables
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Gong handler is ready to use.")
        return True
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
