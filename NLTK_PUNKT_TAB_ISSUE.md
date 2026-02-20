# Docker Image Missing NLTK `punkt_tab` Resource - Models Fail with Tokenization Error

## 🐛 Bug Description

The official MindsDB Docker image (`mindsdb/mindsdb:latest`) is missing the NLTK `punkt_tab` tokenizer resource, causing all models that rely on default text tokenization to fail during inference.

When creating time-series forecasting models from PostgreSQL views and attempting to describe them, MindsDB crashes with:

```
LookupError: Resource punkt_tab not found.
```

This affects production deployments using the containerized version of MindsDB.

---

## 📝 Steps to Reproduce

1. **Start MindsDB using the official Docker image:**
   ```bash
   docker run -d --name mindsdb \
     -p 47334:47334 \
     -p 47335:47335 \
     mindsdb/mindsdb:latest
   ```

2. **Connect MindsDB to a PostgreSQL database:**
   ```sql
   CREATE DATABASE postgres_db
   WITH ENGINE = 'postgres',
   PARAMETERS = {
     "host": "your-host",
     "port": 5432,
     "database": "your_db",
     "user": "your_user",
     "password": "your_password"
   };
   ```

3. **Create a view for time-series forecasting:**
   ```sql
   CREATE OR REPLACE VIEW product_template_sales_per_month AS (
     SELECT 
       product_id,
       date_trunc('month', order_date) AS month,
       SUM(quantity) AS total_sales
     FROM sales_data
     GROUP BY product_id, month
   );
   ```

4. **Create a forecasting model:**
   ```sql
   CREATE MODEL product_sales_forecast
   FROM postgres_db (SELECT * FROM product_template_sales_per_month)
   PREDICT total_sales
   ORDER BY month
   GROUP BY product_id
   HORIZON 3;
   ```

5. **Attempt to describe the model:**
   ```sql
   DESCRIBE product_sales_forecast;
   ```

6. **Observe the error:**
   ```
   LookupError: 
   **********************************************************************
     Resource punkt_tab not found.
     Please use the NLTK Downloader to obtain the resource:
   
       >>> import nltk
       >>> nltk.download('punkt_tab')
   
     For more information see: https://www.nltk.org/data.html
   
     Attempted to load tokenizers/punkt_tab/english/
   
     Searched in:
         - '/root/nltk_data'
         - '/venv/nltk_data'
         - '/usr/share/nltk_data'
         - '/usr/local/share/nltk_data'
         - '/usr/lib/nltk_data'
         - '/usr/local/lib/nltk_data'
   **********************************************************************
   ```

---

## ✅ Expected Behavior

- The MindsDB Docker image should include all essential NLTK tokenizer resources (`punkt` and `punkt_tab`) out of the box
- Models should successfully tokenize text data without requiring manual NLTK resource downloads
- Users should be able to create and describe models without encountering missing tokenizer errors

---

## ❌ Actual Behavior

- Docker container is missing `punkt_tab` tokenizer
- Model creation may succeed, but describing or using the model fails
- Users must manually download NLTK resources inside the container as a workaround

---

## 🔧 Current Workaround

Users can manually fix this by executing inside the running container:

```bash
docker exec -it mindsdb python3 -c "import nltk; nltk.download('punkt_tab')"
```

However, this:
- ❌ Doesn't persist across container restarts
- ❌ Requires manual intervention for each deployment
- ❌ Breaks automation/CI pipelines
- ❌ Creates poor user experience

---

## 🔍 Root Cause Analysis

### Why This Happens

Starting with **NLTK 3.8+**, the punkt tokenizer was split into two separate resources:

1. **`punkt`** - Original tokenizer models (legacy)
2. **`punkt_tab`** - New tabular format with improved performance

The MindsDB Docker image currently downloads only the legacy `punkt` resource during build time, but newer versions of NLTK libraries imported by dependencies (such as preprocessing libraries) now default to using `punkt_tab`.

### Where It Breaks

The issue likely originates from:
- **Dockerfile** - NLTK resources downloaded during image build
- **Dependencies** - Libraries expecting newer NLTK resources
- **Initialization** - No runtime fallback for missing tokenizers

---

## 🛠️ Proposed Solutions

### Solution 1: Add `punkt_tab` to Docker Image (Recommended)

**File:** `docker/mindsdb.Dockerfile`

Add the `punkt_tab` resource to the NLTK download step during image build:

```dockerfile
# Current code (locate the NLTK download section)
RUN python -m nltk.downloader punkt stopwords

# Change to:
RUN python -m nltk.downloader punkt punkt_tab stopwords
```

**Complete patch:**

```diff
--- a/docker/mindsdb.Dockerfile
+++ b/docker/mindsdb.Dockerfile
@@ -XX,7 +XX,7 @@ RUN pip install --no-cache-dir mindsdb
 
 # Download required NLTK data
-RUN python -m nltk.downloader punkt stopwords
+RUN python -m nltk.downloader punkt punkt_tab stopwords
 
 # Set working directory
```

**Pros:**
- ✅ Simple one-line fix
- ✅ Resources bundled in image (no runtime downloads)
- ✅ Consistent across all deployments
- ✅ No performance impact at runtime

**Cons:**
- ⚠️ Slightly larger image size (~1MB)

---

### Solution 2: Runtime Auto-Download (Alternative)

Add automatic NLTK resource downloading with graceful fallback.

**File:** `mindsdb/integrations/libs/llm/utils.py` (or appropriate initialization file)

```python
import nltk
import logging

logger = logging.getLogger(__name__)

def ensure_nltk_resources():
    """
    Ensure required NLTK resources are available.
    Downloads missing resources automatically.
    """
    required_resources = ['punkt', 'punkt_tab', 'stopwords']
    
    for resource in required_resources:
        try:
            nltk.data.find(f'tokenizers/{resource}')
        except LookupError:
            logger.info(f"NLTK resource '{resource}' not found. Downloading...")
            try:
                nltk.download(resource, quiet=True)
                logger.info(f"Successfully downloaded NLTK resource '{resource}'")
            except Exception as e:
                logger.warning(
                    f"Failed to download NLTK resource '{resource}': {e}. "
                    "Some NLP features may not work correctly."
                )

# Call during MindsDB initialization
ensure_nltk_resources()
```

**Where to add:** In `mindsdb/__init__.py` or the main application startup sequence.

**Pros:**
- ✅ Handles any missing NLTK resources dynamically
- ✅ Future-proof against NLTK changes
- ✅ Graceful degradation

**Cons:**
- ⚠️ First-run download delay
- ⚠️ Requires internet access on first container start
- ⚠️ May fail in air-gapped environments

---

### Solution 3: Hybrid Approach (Best Practice)

Combine both approaches:
1. Include common resources in Docker image (Solution 1)
2. Add runtime fallback for missing resources (Solution 2)

This provides:
- ✅ Fast startup (resources pre-installed)
- ✅ Resilience (auto-download if missing)
- ✅ Future compatibility

---

## 🧪 Validation & Testing

### Test Case 1: Verify NLTK Resources in Image

```bash
# After building the image
docker run --rm mindsdb/mindsdb:latest \
  python3 -c "import nltk; nltk.data.find('tokenizers/punkt_tab')"

# Should exit with code 0 (success)
```

### Test Case 2: E2E Model Creation

```python
# tests/integration/test_nltk_tokenization.py

import unittest
from mindsdb_sql import parse_sql
from tests.integration.test_base import BaseIntegrationTest

class TestNLTKTokenization(BaseIntegrationTest):
    """Test that NLTK resources are available for model creation."""
    
    def test_punkt_tab_available(self):
        """Verify punkt_tab tokenizer is accessible."""
        import nltk
        try:
            nltk.data.find('tokenizers/punkt_tab')
        except LookupError:
            self.fail("punkt_tab tokenizer not found in container")
    
    def test_model_with_text_features(self):
        """Test model creation with text tokenization."""
        # Create test model that requires tokenization
        query = """
        CREATE MODEL test_nlp_model
        FROM test_db (SELECT text_column, target FROM test_table)
        PREDICT target
        """
        # Should not raise LookupError
        result = self.command_executor.execute_command(parse_sql(query))
        self.assertIsNone(result.error)
```

### Test Case 3: CI/CD Validation

Add to `.github/workflows/docker-build.yml`:

```yaml
- name: Validate NLTK Resources
  run: |
    docker run --rm mindsdb/mindsdb:latest \
      python3 -c "
import nltk
import sys

required = ['punkt', 'punkt_tab', 'stopwords']
missing = []

for resource in required:
    try:
        nltk.data.find(f'tokenizers/{resource}')
        print(f'✓ {resource}')
    except LookupError:
        print(f'✗ {resource} - MISSING')
        missing.append(resource)

if missing:
    print(f'\nError: Missing NLTK resources: {missing}')
    sys.exit(1)
"
```

---

## 📦 Implementation Plan

### Phase 1: Quick Fix (Immediate)
1. Update `docker/mindsdb.Dockerfile` to include `punkt_tab`
2. Rebuild and publish patched Docker image
3. Add release notes mentioning the fix

### Phase 2: Robust Solution (Next Release)
1. Implement runtime resource checker (Solution 2)
2. Add integration tests for NLTK resources
3. Add CI validation step

### Phase 3: Documentation (Ongoing)
1. Update Docker documentation with NLTK resource info
2. Add troubleshooting guide for NLTK issues
3. Document how to add custom NLTK resources

---

## 📋 Pull Request Checklist

- [ ] Add `punkt_tab` to Dockerfile NLTK downloads
- [ ] Update `requirements.txt` if NLTK version needs pinning
- [ ] Add integration test for NLTK tokenization
- [ ] Add CI step to validate NLTK resources in built image
- [ ] Update Docker documentation
- [ ] Add entry to CHANGELOG
- [ ] Test with existing models to ensure no regression

---

## 💬 Additional Context

### Why NLTK Split `punkt` and `punkt_tab`

From NLTK 3.8 release notes:

> The punkt tokenizer has been refactored to use a tabular format (`punkt_tab`) 
> for improved performance and easier maintenance. The legacy `punkt` format is 
> deprecated but still available for backward compatibility.

Many libraries now default to `punkt_tab`, causing issues when only the legacy version is available.

### Impact Assessment

**Severity:** High  
**Affected Users:** All Docker deployments using NLP features  
**Affected Versions:** All current Docker tags  
**Workaround Available:** Yes (manual download)  

### Related Issues

This may be related to or cause similar issues in:
- Text preprocessing pipelines
- Sentiment analysis models
- Any feature using NLTK tokenization

---

## 🙋 Questions for Maintainers

1. Should we pin NLTK to a specific version to prevent future breaking changes?
2. Do we want to include ALL NLTK corpora or just essential ones?
3. Should we add a mechanism to allow users to specify additional NLTK resources in config?

---

## 🔗 References

- [NLTK Data Documentation](https://www.nltk.org/data.html)
- [NLTK 3.8 Release Notes](https://github.com/nltk/nltk/releases/tag/3.8)
- [MindsDB Docker Images](https://hub.docker.com/r/mindsdb/mindsdb)

---

## 👤 Reporter Information

- **Environment:** Docker (mindsdb/mindsdb:latest)
- **MindsDB Version:** [Check with `SELECT version()` in MindsDB]
- **Python Version:** [Check in container]
- **NLTK Version:** [Check with `import nltk; print(nltk.__version__)`]

---

**I'm happy to submit a PR with the fix if this approach is approved by maintainers!** 🚀
