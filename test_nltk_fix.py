"""
Quick test script to verify NLTK punkt_tab fix works locally.
This simulates what happens in the Docker container.
"""

import sys
import os

def test_nltk_imports():
    """Test that NLTK can be imported."""
    print("🔍 Testing NLTK installation...")
    try:
        import nltk
        print("✅ NLTK is installed")
        print(f"   Version: {nltk.__version__}")
        return True
    except ImportError:
        print("❌ NLTK is NOT installed locally")
        print("   This is OK - NLTK will be installed in Docker")
        return False

def test_punkt_tab_availability():
    """Test if punkt_tab resource is available."""
    print("\n🔍 Testing punkt_tab availability...")
    try:
        import nltk
        nltk.data.find('tokenizers/punkt_tab')
        print("✅ punkt_tab tokenizer is available")
        return True
    except LookupError:
        print("❌ punkt_tab tokenizer NOT found")
        print("   Attempting to download...")
        try:
            nltk.download('punkt_tab', quiet=True)
            print("✅ punkt_tab downloaded successfully")
            return True
        except:
            print("❌ Could not download punkt_tab")
            return False
    except ImportError:
        print("⚠️  NLTK not installed - skipping")
        return None

def test_punkt_availability():
    """Test if legacy punkt resource is available."""
    print("\n🔍 Testing punkt (legacy) availability...")
    try:
        import nltk
        nltk.data.find('tokenizers/punkt')
        print("✅ punkt tokenizer is available")
        return True
    except LookupError:
        print("❌ punkt tokenizer NOT found")
        return False
    except ImportError:
        print("⚠️  NLTK not installed - skipping")
        return None

def test_tokenization():
    """Test actual tokenization functionality."""
    print("\n🔍 Testing tokenization functionality...")
    try:
        from nltk.tokenize import word_tokenize, sent_tokenize
        
        # Test word tokenization
        test_text = "This is a test sentence. It should work!"
        words = word_tokenize(test_text)
        print(f"✅ Word tokenization works: {len(words)} tokens")
        print(f"   Tokens: {words[:5]}...")
        
        # Test sentence tokenization
        sentences = sent_tokenize(test_text)
        print(f"✅ Sentence tokenization works: {len(sentences)} sentences")
        
        return True
    except Exception as e:
        print(f"❌ Tokenization failed: {e}")
        return False

def test_utils_module():
    """Test the nltk_utils module."""
    print("\n🔍 Testing nltk_utils module...")
    try:
        from mindsdb.utilities.nltk_utils import (
            check_nltk_resource,
            ensure_nltk_resources,
            initialize_nltk
        )
        print("✅ nltk_utils module imported successfully")
        
        # Test initialization
        print("\n   Running initialize_nltk()...")
        initialize_nltk(auto_download=False)
        print("✅ NLTK initialization completed")
        
        return True
    except ImportError as e:
        print(f"❌ Could not import nltk_utils: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Error during initialization: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("🧪 NLTK Fix Verification Script")
    print("=" * 60)
    
    results = {
        "NLTK Installation": test_nltk_imports(),
        "punkt_tab Resource": test_punkt_tab_availability(),
        "punkt Resource": test_punkt_availability(),
        "Tokenization": test_tokenization(),
        "Utils Module": test_utils_module(),
    }
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  SKIP"
        print(f"{test_name:25} {status}")
    
    print("\n" + "=" * 60)
    
    # Check if NLTK is installed locally
    if results["NLTK Installation"] is False:
        print("\n💡 NLTK is not installed locally (this is normal)")
        print("   To test locally, install NLTK:")
        print("   pip install nltk")
        print("\n   The REAL test is building the Docker image!")
        return
    
    # Final verdict
    failed = [k for k, v in results.items() if v is False]
    if failed:
        print(f"\n❌ Some tests failed: {', '.join(failed)}")
        print("   This might indicate an issue with your local setup")
    else:
        print("\n✅ All tests passed!")
        print("   Your NLTK fix appears to be working correctly")

if __name__ == "__main__":
    main()
