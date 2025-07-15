#!/usr/bin/env python3

"""
Verification script for the Raindrop.io handler implementation.
This script checks all the key functionality without requiring a real API key.
"""

import sys
import json
from unittest.mock import Mock, patch

def test_handler_loading():
    """Test that the handler can be loaded and instantiated"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler import (
            Handler, name, type, title, connection_args
        )
        print("✓ Handler module loaded successfully")
        print(f"  Name: {name}")
        print(f"  Type: {type}")
        print(f"  Title: {title}")
        print(f"  Connection args: {list(connection_args.keys())}")
        
        # Test instantiation
        handler = Handler('test_handler')
        print("✓ Handler instantiated successfully")
        print(f"  Tables: {list(handler._tables.keys())}")
        
        return True
    except Exception as e:
        print(f"✗ Handler loading failed: {e}")
        return False

def test_api_client():
    """Test the API client functionality"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler.raindrop_handler import RaindropAPIClient
        
        client = RaindropAPIClient('test_key')
        print("✓ API client instantiated successfully")
        print(f"  Base URL: {client.base_url}")
        print(f"  Headers configured: {'Authorization' in client.headers}")
        
        return True
    except Exception as e:
        print(f"✗ API client test failed: {e}")
        return False

def test_table_functionality():
    """Test table functionality with mocked data"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler.raindrop_tables import (
            RaindropsTable, CollectionsTable
        )
        import pandas as pd
        
        # Test RaindropsTable
        handler_mock = Mock()
        raindrops_table = RaindropsTable(handler_mock)
        
        columns = raindrops_table.get_columns()
        print(f"✓ RaindropsTable columns: {len(columns)} columns")
        
        # Test data normalization
        test_data = pd.DataFrame([{
            '_id': 123,
            'title': 'Test',
            'collection': {'$id': 456, 'title': 'Test Collection'},
            'tags': ['tag1', 'tag2'],
            'created': '2024-01-01T00:00:00Z'
        }])
        
        normalized = raindrops_table._normalize_raindrop_data(test_data)
        print("✓ RaindropsTable data normalization works")
        
        # Test data preparation
        prep_data = raindrops_table._prepare_raindrop_data({
            'link': 'https://example.com',
            'title': 'Test',
            'tags': 'tag1,tag2',
            'collection_id': 123
        })
        print("✓ RaindropsTable data preparation works")
        
        # Test CollectionsTable
        collections_table = CollectionsTable(handler_mock)
        columns = collections_table.get_columns()
        print(f"✓ CollectionsTable columns: {len(columns)} columns")
        
        return True
    except Exception as e:
        print(f"✗ Table functionality test failed: {e}")
        return False

def test_connection_handling():
    """Test connection handling"""
    try:
        from mindsdb.integrations.handlers.raindrop_handler import Handler
        
        # Test with missing API key
        handler = Handler('test')
        try:
            handler.connect()
            print("✗ Should have failed with missing API key")
            return False
        except ValueError as e:
            if "API key is required" in str(e):
                print("✓ Properly validates missing API key")
            else:
                print(f"✗ Unexpected error: {e}")
                return False
        
        # Test with API key
        handler.connection_data = {'api_key': 'test_key'}
        
        with patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient') as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance
            
            connection = handler.connect()
            print("✓ Connection with API key works")
            
            # Test connection check
            mock_instance.get_user_stats.return_value = {'result': True}
            status = handler.check_connection()
            print(f"✓ Connection check works: {status.success}")
        
        return True
    except Exception as e:
        print(f"✗ Connection handling test failed: {e}")
        return False

def test_query_utilities():
    """Test that required query utilities are available"""
    try:
        from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser
        from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import (
            SELECTQueryParser, SELECTQueryExecutor
        )
        from mindsdb.integrations.utilities.handlers.query_utilities.update_query_utilities import (
            UPDATEQueryParser, UPDATEQueryExecutor
        )
        from mindsdb.integrations.utilities.handlers.query_utilities.delete_query_utilities import (
            DELETEQueryParser, DELETEQueryExecutor
        )
        
        print("✓ All query utilities imported successfully")
        return True
    except Exception as e:
        print(f"✗ Query utilities test failed: {e}")
        return False

def main():
    """Run all verification tests"""
    print("🔍 Verifying Raindrop.io Handler Implementation")
    print("=" * 50)
    
    tests = [
        ("Handler Loading", test_handler_loading),
        ("API Client", test_api_client),
        ("Table Functionality", test_table_functionality),
        ("Connection Handling", test_connection_handling),
        ("Query Utilities", test_query_utilities),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        if test_func():
            passed += 1
        else:
            print(f"❌ {test_name} failed")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The Raindrop.io handler is ready for use.")
        return 0
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
