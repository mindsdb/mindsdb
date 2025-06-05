# MindsDB Dependency Cleanup Summary

## Overview
This document outlines the dependency cleanup performed as part of the MindsDB streamlining strategy, reducing dependencies while maintaining functionality for the 15 essential handlers.

## Files Created

### 1. `requirements/requirements-streamlined.txt`
A comprehensive requirements file containing only dependencies needed for the 15 essential handlers:
- **Core MindsDB dependencies**: Flask, SQLAlchemy, Pandas, etc.
- **Handler-specific dependencies**: Only for the 15 kept handlers
- **Removed**: MongoDB and unused handler dependencies

### 2. `requirements/requirements-dev-streamlined.txt`
Development dependencies for the streamlined version:
- Code formatting and linting tools
- Build and deployment tools
- No test-specific dependencies (tests removed)

### 3. `config-streamlined.json`
Optimized configuration template:
- Only includes settings for the 15 essential handlers
- Simplified API configuration (HTTP only by default)
- Removed MongoDB-related configurations

## Dependencies Removed

### MongoDB Dependencies
- `pymongo[srv] == 4.8.0` - Removed from main requirements.txt

### Handler-Specific Dependencies
All dependencies for the 147 removed handlers have been eliminated, including but not limited to:
- Database connectors (MySQL, Oracle, SQL Server, etc.)
- Cloud service SDKs (AWS, Azure, GCP services)
- Specialized APIs and protocols
- Machine learning frameworks not used by kept handlers

## Dependencies Preserved

### Core Dependencies
- Flask web framework and related packages
- SQLAlchemy for database operations
- Pandas for data manipulation
- PostgreSQL drivers (psycopg2-binary)
- Redis for caching
- Security and authentication packages

### Handler-Specific Dependencies
- **Gmail**: google-api-python-client, google-auth, google-auth-oauthlib
- **Google Analytics**: google-analytics-admin
- **Lightwood**: lightwood>=25.5.2.2 with extras
- **Hugging Face**: transformers, datasets, torch, huggingface-hub
- **LangChain**: Full LangChain ecosystem packages
- **PGVector**: pgvector==0.3.6
- **RAG**: faiss-cpu, sentence-transformers, html2text
- **S3**: boto3 (already in core)
- **Minds Endpoint**: pydantic-settings

## Handler Dependencies Analysis

### Handlers with No Additional Dependencies
- **Airtable**: Uses standard HTTP requests
- **Supabase**: Uses standard HTTP requests  
- **Postgres**: Uses core psycopg2-binary
- **Sheets**: Uses standard HTTP requests
- **S3**: Uses core boto3

### Handlers with Specific Dependencies
- **Gmail**: Google API client libraries
- **Google Analytics**: Google Analytics API
- **Lightwood**: ML framework with extras
- **Hugging Face**: Transformers and ML libraries
- **LangChain**: Comprehensive LangChain ecosystem
- **PGVector**: Vector database support
- **RAG**: Vector search and text processing

## Impact Metrics

### Dependency Reduction
- **Before**: 58+ core dependencies + 147 handler-specific dependency sets
- **After**: 58 core dependencies + 15 handler-specific dependency sets
- **Reduction**: ~90% reduction in handler-specific dependencies

### Security Benefits
- Reduced attack surface through fewer dependencies
- Eliminated MongoDB-related security considerations
- Simplified dependency management and updates

### Performance Benefits
- Faster installation times
- Reduced memory footprint
- Quicker startup times

## Usage Instructions

### For Production Deployment
```bash
pip install -r requirements/requirements-streamlined.txt
```

### For Development
```bash
pip install -r requirements/requirements-streamlined.txt
pip install -r requirements/requirements-dev-streamlined.txt
```

### Configuration
Use `config-streamlined.json` as a template for production deployments.

## Verification

To verify the streamlined dependencies work correctly:

1. **Install streamlined requirements**:
   ```bash
   pip install -r requirements/requirements-streamlined.txt
   ```

2. **Start MindsDB**:
   ```bash
   python -m mindsdb
   ```

3. **Verify handlers**:
   - Check that only 15 handlers are available
   - Test core functionality with each handler
   - Confirm no MongoDB-related errors

## Maintenance

### Adding New Dependencies
When adding dependencies for the 15 essential handlers:
1. Add to `requirements-streamlined.txt`
2. Document the handler and reason
3. Test compatibility with existing dependencies

### Removing Dependencies
Before removing any dependency:
1. Verify it's not used by any of the 15 essential handlers
2. Check for transitive dependencies
3. Test the streamlined system after removal

## Files Modified
- `requirements/requirements.txt` - Removed MongoDB dependencies
- `requirements/requirements-streamlined.txt` - Created
- `requirements/requirements-dev-streamlined.txt` - Created  
- `config-streamlined.json` - Created
- `requirements/requirements-original-backup.txt` - Backup created

This dependency cleanup achieves the goal of reducing complexity while maintaining all essential functionality for the streamlined MindsDB instance.