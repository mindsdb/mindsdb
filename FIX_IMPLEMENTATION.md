# NLTK punkt_tab Fix - Implementation Files

This directory contains all the code changes needed to fix the NLTK `punkt_tab` missing resource bug in MindsDB Docker images.

## 📁 Files Modified/Created

1. **docker/mindsdb.Dockerfile** - Add punkt_tab to image build
2. **mindsdb/utilities/nltk_utils.py** - Runtime resource checker (NEW FILE)
3. **tests/integration/test_nltk_resources.py** - Integration tests (NEW FILE)
4. **.github/workflows/docker-build.yml** - CI validation

---

## 🔧 Fix Implementation

### File 1: docker/mindsdb.Dockerfile

**Locate the NLTK download section and update it:**

```dockerfile
# BEFORE (search for this line):
RUN python -m nltk.downloader punkt stopwords

# AFTER (replace with this):
RUN python -m nltk.downloader punkt punkt_tab stopwords
```

**Complete context patch:**

```dockerfile
# Install MindsDB and dependencies
RUN pip install --no-cache-dir mindsdb

# Download required NLTK data
# Updated to include punkt_tab for NLTK 3.8+ compatibility
RUN python -m nltk.downloader \
    punkt \
    punkt_tab \
    stopwords \
    averaged_perceptron_tagger

# Set working directory
WORKDIR /root
```

---

### File 2: mindsdb/utilities/nltk_utils.py (NEW FILE)

**Create this file to handle runtime NLTK resource management:**

```python
"""
NLTK Resource Management Utilities

Ensures required NLTK resources are available at runtime.
Provides graceful fallback with automatic downloading.
"""

import os
import nltk
from typing import List, Dict
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Required NLTK resources for MindsDB operations
REQUIRED_NLTK_RESOURCES = {
    'tokenizers/punkt': 'punkt',
    'tokenizers/punkt_tab': 'punkt_tab',
    'corpora/stopwords': 'stopwords',
    'taggers/averaged_perceptron_tagger': 'averaged_perceptron_tagger'
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
        nltk.data.find(resource_path)
        return True
    except LookupError:
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
        Dictionary mapping resource paths to availability status
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
    return nltk.data.path


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
    # Determine auto_download setting
    if auto_download is None:
        # Default to True if running in Docker
        is_docker = os.path.exists('/.dockerenv')
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
        logger.error(
            f"Critical NLTK resources missing: {missing_critical}. "
            f"NLP features may not work correctly."
        )


# Initialize on module import if in production environment
if os.getenv('MINDSDB_NLTK_INIT_ON_IMPORT', 'false').lower() in ('true', '1', 'yes'):
    initialize_nltk()
```

---

### File 3: mindsdb/__init__.py (MODIFICATION)

**Add NLTK initialization to MindsDB startup:**

Find the initialization section and add:

```python
# Add this import at the top
from mindsdb.utilities.nltk_utils import initialize_nltk

# Add this in the initialization section (look for other setup code)
# Initialize NLTK resources
try:
    initialize_nltk(auto_download=True)
except Exception as e:
    logger.warning(f"NLTK initialization failed: {e}. NLP features may be limited.")
```

---

### File 4: tests/integration/test_nltk_resources.py (NEW FILE)

**Create comprehensive integration tests:**

```python
"""
Integration tests for NLTK resource availability.

Ensures that all required NLTK resources are available
in the MindsDB environment (especially Docker containers).
"""

import unittest
import nltk
from mindsdb.utilities.nltk_utils import (
    check_nltk_resource,
    ensure_nltk_resources,
    REQUIRED_NLTK_RESOURCES,
    get_nltk_data_paths
)


class TestNLTKResources(unittest.TestCase):
    """Test NLTK resource availability."""
    
    def test_punkt_tab_available(self):
        """Test that punkt_tab tokenizer is available."""
        available = check_nltk_resource('tokenizers/punkt_tab')
        self.assertTrue(
            available,
            "punkt_tab tokenizer not found. This will cause tokenization failures."
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
        
        self.assertEqual(
            len(missing), 0,
            f"Missing NLTK resources: {missing}. "
            f"Update Dockerfile to include: {', '.join(missing)}"
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
            self.fail(f"Tokenization failed due to missing NLTK resource: {e}")
    
    def test_sentence_tokenization(self):
        """Test sentence tokenization (also uses punkt_tab)."""
        try:
            from nltk.tokenize import sent_tokenize
            
            test_text = "First sentence. Second sentence! Third sentence?"
            sentences = sent_tokenize(test_text)
            
            self.assertEqual(len(sentences), 3)
            
        except LookupError as e:
            self.fail(f"Sentence tokenization failed: {e}")


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


if __name__ == '__main__':
    unittest.main()
```

---

### File 5: .github/workflows/docker-build.yml (MODIFICATION)

**Add NLTK validation step to Docker CI:**

```yaml
# Add this step after the Docker image is built
# (locate the section where Docker image is tested)

- name: Validate NLTK Resources in Image
  run: |
    echo "Validating NLTK resources in Docker image..."
    docker run --rm mindsdb/mindsdb:${{ github.sha }} \
      python3 -c "
import sys
import nltk

# Required NLTK resources
REQUIRED = {
    'tokenizers/punkt': 'punkt',
    'tokenizers/punkt_tab': 'punkt_tab',
    'corpora/stopwords': 'stopwords'
}

print('Checking NLTK resources...')
print('-' * 50)

missing = []
for path, name in REQUIRED.items():
    try:
        nltk.data.find(path)
        print(f'✓ {name:30} FOUND')
    except LookupError:
        print(f'✗ {name:30} MISSING')
        missing.append(name)

print('-' * 50)

if missing:
    print(f'\n❌ ERROR: Missing NLTK resources: {missing}')
    print('\nTo fix, update docker/mindsdb.Dockerfile:')
    print(f'RUN python -m nltk.downloader {\" \".join(missing)}')
    sys.exit(1)
else:
    print('\n✅ All required NLTK resources are available')
    sys.exit(0)
"

- name: Test NLTK Tokenization in Container
  run: |
    echo "Testing NLTK tokenization..."
    docker run --rm mindsdb/mindsdb:${{ github.sha }} \
      python3 -c "
from nltk.tokenize import word_tokenize, sent_tokenize

# Test word tokenization
text = 'Hello world! This is a test.'
tokens = word_tokenize(text)
print(f'Word tokens: {tokens}')

# Test sentence tokenization  
sentences = sent_tokenize(text)
print(f'Sentences: {sentences}')

print('✅ Tokenization working correctly')
"
```

---

## 🧪 Testing the Fix

### Local Testing (Before Docker Build)

```bash
# 1. Test NLTK utilities
cd mindsdb
python -m pytest tests/integration/test_nltk_resources.py -v

# 2. Test manual initialization
python -c "from mindsdb.utilities.nltk_utils import initialize_nltk; initialize_nltk()"
```

### Docker Testing (After Build)

```bash
# 1. Build the image
docker build -t mindsdb/mindsdb:test -f docker/mindsdb.Dockerfile .

# 2. Validate NLTK resources
docker run --rm mindsdb/mindsdb:test \
  python3 -c "import nltk; nltk.data.find('tokenizers/punkt_tab')"

# 3. Test tokenization
docker run --rm mindsdb/mindsdb:test \
  python3 -c "from nltk.tokenize import word_tokenize; print(word_tokenize('Test sentence.'))"

# 4. Run full integration tests
docker run --rm mindsdb/mindsdb:test \
  python -m pytest tests/integration/test_nltk_resources.py -v
```

---

## 📋 Checklist Before Submitting PR

- [ ] Updated `docker/mindsdb.Dockerfile` with punkt_tab
- [ ] Created `mindsdb/utilities/nltk_utils.py`
- [ ] Modified `mindsdb/__init__.py` to initialize NLTK
- [ ] Created integration tests in `tests/integration/test_nltk_resources.py`
- [ ] Updated CI workflow with NLTK validation
- [ ] Tested locally with pytest
- [ ] Built and tested Docker image
- [ ] Verified tokenization works in container
- [ ] Updated CHANGELOG.md
- [ ] Updated documentation if needed

---

## 🚀 Ready to Create PR!

All code is ready. Next steps:

1. Review all changes
2. Run tests locally
3. Commit changes
4. Push to your fork
5. Create PR using `NLTK_PUNKT_TAB_ISSUE.md` as the description
