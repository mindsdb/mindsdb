"""
Integration tests for NLTK resource availability.

Ensures that all required NLTK resources are available
in the MindsDB environment (especially Docker containers).
"""

import unittest
import sys

try:
    import nltk
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

from mindsdb.utilities.nltk_utils import (
    check_nltk_resource,
    ensure_nltk_resources,
    REQUIRED_NLTK_RESOURCES,
    get_nltk_data_paths,
    initialize_nltk
)


@unittest.skipUnless(NLTK_AVAILABLE, "NLTK not installed")
class TestNLTKResources(unittest.TestCase):
    """Test NLTK resource availability."""
    
    def test_punkt_tab_available(self):
        """Test that punkt_tab tokenizer is available."""
        available = check_nltk_resource('tokenizers/punkt_tab')
        self.assertTrue(
            available,
            "punkt_tab tokenizer not found. This will cause tokenization failures. "
            "Fix: Update docker/mindsdb.Dockerfile to include 'punkt_tab' in nltk downloads"
        )
    
    def test_punkt_available(self):
        """Test that legacy punkt tokenizer is available."""
        available = check_nltk_resource('tokenizers/punkt')
        self.assertTrue(
            available,
            "punkt tokenizer not found. This will cause fallback issues."
        )
    
    def test_all_required_resources(self):
        """Test that all required NLTK resources are available."""
        results = ensure_nltk_resources(auto_download=False)
        
        missing = [name for name, available in results.items() if not available]
        
        if missing:
            self.fail(
                f"Missing NLTK resources: {missing}. "
                f"Update docker/mindsdb.Dockerfile NLTK downloads to include: {' '.join(missing)}"
            )
    
    def test_nltk_data_paths_exist(self):
        """Test that NLTK data paths exist and are accessible."""
        paths = get_nltk_data_paths()
        
        self.assertGreater(
            len(paths), 0,
            "No NLTK data paths configured"
        )
        
        # At least one path should exist
        import os
        existing_paths = [p for p in paths if os.path.exists(p)]
        
        self.assertGreater(
            len(existing_paths), 0,
            f"None of the NLTK data paths exist: {paths}"
        )
    
    def test_punkt_tab_tokenization(self):
        """Test that punkt_tab can actually tokenize text."""
        try:
            from nltk.tokenize import word_tokenize
            
            test_text = "This is a test sentence. It should be tokenized correctly!"
            tokens = word_tokenize(test_text)
            
            self.assertIsInstance(tokens, list)
            self.assertGreater(len(tokens), 0)
            self.assertIn("test", tokens)
            
        except LookupError as e:
            self.fail(
                f"Tokenization failed due to missing NLTK resource: {e}. "
                f"This is the exact error users are experiencing. "
                f"Fix required in Dockerfile."
            )
    
    def test_sentence_tokenization(self):
        """Test sentence tokenization (also uses punkt_tab)."""
        try:
            from nltk.tokenize import sent_tokenize
            
            test_text = "First sentence. Second sentence! Third sentence?"
            sentences = sent_tokenize(test_text)
            
            self.assertEqual(len(sentences), 3)
            
        except LookupError as e:
            self.fail(f"Sentence tokenization failed: {e}")
    
    def test_initialize_nltk_no_error(self):
        """Test that NLTK initialization doesn't raise errors."""
        try:
            initialize_nltk(auto_download=False)
        except Exception as e:
            self.fail(f"NLTK initialization raised unexpected error: {e}")


@unittest.skipUnless(NLTK_AVAILABLE, "NLTK not installed")
class TestNLTKUtilities(unittest.TestCase):
    """Test NLTK utility functions."""
    
    def test_check_nltk_resource_valid(self):
        """Test checking a valid resource."""
        # punkt should always be available
        result = check_nltk_resource('tokenizers/punkt')
        self.assertTrue(result)
    
    def test_check_nltk_resource_invalid(self):
        """Test checking an invalid resource."""
        result = check_nltk_resource('nonexistent/resource')
        self.assertFalse(result)
    
    def test_ensure_nltk_resources_returns_dict(self):
        """Test that ensure_nltk_resources returns a status dictionary."""
        results = ensure_nltk_resources(auto_download=False)
        
        self.assertIsInstance(results, dict)
        self.assertGreater(len(results), 0)
        
        # All values should be boolean
        for value in results.values():
            self.assertIsInstance(value, bool)
    
    def test_get_nltk_data_paths(self):
        """Test retrieving NLTK data paths."""
        paths = get_nltk_data_paths()
        self.assertIsInstance(paths, list)


if __name__ == '__main__':
    # Run tests
    unittest.main()
