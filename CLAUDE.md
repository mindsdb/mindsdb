# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MindsDB is an AI SQL server that enables developers to build AI tools that need access to real-time data. It provides a unified interface to connect, unify, and respond to questions over large-scale federated data—spanning databases, data warehouses, and SaaS applications.

## Key Commands

### Development Setup
```bash
# Install MindsDB in development mode
make install_mindsdb

# Install a specific handler (set HANDLER_NAME environment variable)
make install_handler HANDLER_NAME=<handler_name>

# Install pre-commit hooks
make precommit
```

### Running MindsDB
```bash
# Run MindsDB locally
make run_mindsdb
# Or directly
python -m mindsdb

# Run specific APIs (comma-separated list)
python -m mindsdb --api=http,mysql,mcp

# Run with Docker
make build_docker
make run_docker
```

### Testing
```bash
# Run integration tests
make integration_tests

# Run integration tests with slow tests included
make integration_tests_slow

# Run integration tests in debug mode
make integration_tests_debug

# Run unit tests
make unit_tests

# Run unit tests with slow tests included
make unit_tests_slow

# Run unit tests in debug mode
make unit_tests_debug
```

### Code Quality
```bash
# Run pre-commit hooks on changed files
make precommit

# Format code using pre-commit hooks
make format

# Check requirements and print statements
make check
```

### Linting
```bash
# Ruff is configured in pyproject.toml
ruff check .
ruff format .
```

## Architecture Overview

MindsDB follows a modular architecture with these core components:

### Core Structure
- **`mindsdb/api/`** - API implementations (HTTP, MySQL, Postgres, MongoDB, MCP, LiteLLM)
- **`mindsdb/integrations/`** - Data and AI integrations/handlers
- **`mindsdb/interfaces/`** - Internal interfaces (agents, models, databases, jobs, knowledge bases)
- **`mindsdb/utilities/`** - Shared utilities and configuration

### API Layers
- **HTTP API** - REST API and web GUI (port 47334 by default)
- **MySQL API** - MySQL-compatible protocol interface
- **Postgres API** - PostgreSQL-compatible protocol interface  
- **MongoDB API** - MongoDB-compatible protocol interface
- **MCP (Model Context Protocol)** - Integration with MCP-compatible tools like Cursor
- **LiteLLM** - LLM proxy service

### Integration System
- **Data Handlers** - Connect to databases, files, and data sources (100+ integrations)
- **AI Handlers** - Integrate with ML/AI providers (OpenAI, Anthropic, HuggingFace, etc.)
- **App Handlers** - Connect to SaaS applications (Slack, Gmail, GitHub, etc.)

### Core Interfaces
- **Agents** - Autonomous AI agents that can interact with data and tools
- **Models** - ML model management and inference
- **Knowledge Bases** - Vector storage and retrieval for unstructured data
- **Jobs** - Scheduled task execution
- **Skills** - Reusable agent capabilities
- **Triggers** - Event-driven automation

### Process Management
MindsDB uses multiprocessing with a main process that spawns and manages API processes. The `__main__.py` file orchestrates process lifecycle, including automatic restarts on failure.

## Development Guidelines

### Handler Development
- Each integration lives in `mindsdb/integrations/handlers/<name>_handler/`
- Handlers extend base classes from `mindsdb/integrations/libs/`
- Include `requirements.txt` for handler-specific dependencies
- Add connection arguments in `connection_args.py`

### Testing
- Integration tests go in `tests/integration/`
- Unit tests go in `tests/unit/`
- Use pytest with parallel execution (`-n auto`)
- Executor tests run separately due to process isolation needs

### Configuration
- Main config in `mindsdb/utilities/config.py`
- Environment variables prefixed with `MINDSDB_`
- Default APIs: HTTP, MySQL, MCP, A2A
- Jobs and tasks run automatically unless disabled

### Database Migrations
- Located in `mindsdb/migrations/versions/`
- Uses Alembic for schema management
- Run automatically on startup in local mode

## Important Notes

- Python 3.10+ required (configured in setup.py)
- Uses Ruff for linting with specific exclusions for complexity and line length
- Pre-commit hooks automatically format code and run checks
- Multiprocessing uses "spawn" method for compatibility
- Cloud deployments use different configuration paths
- Handler requirements are dynamically loaded based on `default_handlers.txt`