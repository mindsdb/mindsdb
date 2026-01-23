#!/usr/bin/env bash
#
# Local Unit Test Runner for macOS
# Replicates the GitHub Actions workflow from .github/workflows/tests_unit.yml
#
# Usage:
#   ./run_unit_tests.sh              # Run all steps (checks + tests)
#   ./run_unit_tests.sh --tests-only # Skip code checks, only run tests
#   ./run_unit_tests.sh --checks-only # Only run static code checks
#   ./run_unit_tests.sh --fast       # Run tests without --runslow flag
#   ./run_unit_tests.sh --help       # Show this help
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
RUN_CHECKS=true
RUN_TESTS=true
RUN_SLOW=true
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"

# Handlers to install (from the workflow)
HANDLERS_TO_INSTALL=(
    postgres
    mysql
    salesforce
    snowflake
    timescaledb
    mssql
    oracle
    slack
    redshift
    bigquery
    clickhouse
    web
    databricks
    github
    ms_teams
    statsforecast
    chromadb
    confluence
)

print_header() {
    echo -e "\n${BLUE}============================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}============================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

show_help() {
    echo "Local Unit Test Runner for macOS"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --tests-only    Skip static code checks, only run unit tests"
    echo "  --checks-only   Only run static code checks (no tests)"
    echo "  --fast          Run tests without --runslow flag (faster)"
    echo "  --help          Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  PYTHON_VERSION  Python version to use (default: 3.11)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run everything"
    echo "  $0 --tests-only       # Just run tests"
    echo "  $0 --fast             # Run fast tests only"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --tests-only)
            RUN_CHECKS=false
            shift
            ;;
        --checks-only)
            RUN_TESTS=false
            shift
            ;;
        --fast)
            RUN_SLOW=false
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Change to repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_header "MindsDB Unit Test Runner"

# Check for uv
if ! command -v uv &> /dev/null; then
    print_error "uv is not installed. Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi
print_success "Found uv: $(uv --version)"

# Check Python version
PYTHON_PATH=$(uv python find "$PYTHON_VERSION" 2>/dev/null || true)
if [[ -z "$PYTHON_PATH" ]]; then
    print_warning "Python $PYTHON_VERSION not found, installing..."
    uv python install "$PYTHON_VERSION"
    PYTHON_PATH=$(uv python find "$PYTHON_VERSION")
fi
print_success "Using Python: $PYTHON_PATH"

# Create/activate virtual environment if not already in one
if [[ -z "$VIRTUAL_ENV" ]]; then
    if [[ ! -d "env" ]]; then
        print_header "Creating virtual environment"
        uv venv env --python "$PYTHON_VERSION"
    fi
    print_success "Activating virtual environment"
    source env/bin/activate
fi

#
# STATIC CODE CHECKS
#
if [[ "$RUN_CHECKS" == "true" ]]; then
    print_header "Running Static Code Checks"

    # Check for print statements
    echo "Checking for print statements..."
    if uv run tests/scripts/check_print_statements.py; then
        print_success "No forbidden print statements found"
    else
        print_error "Found forbidden print statements"
        exit 1
    fi

    # Install dev requirements for pre-commit
    echo "Installing dev requirements..."
    uv pip install -r requirements/requirements-dev.txt

    # Run pre-commit on changed files
    echo "Running pre-commit on changed files..."
    # Get the base branch (usually main or staging)
    BASE_BRANCH=$(git rev-parse --abbrev-ref HEAD@{upstream} 2>/dev/null | sed 's|origin/||' || echo "main")
    
    if git diff --quiet HEAD 2>/dev/null; then
        # No uncommitted changes, run on commits since base
        MERGE_BASE=$(git merge-base HEAD "origin/$BASE_BRANCH" 2>/dev/null || echo "HEAD~1")
        if pre-commit run --show-diff-on-failure --color=always --from-ref "$MERGE_BASE" --to-ref HEAD; then
            print_success "Pre-commit checks passed"
        else
            print_warning "Pre-commit checks had issues (continuing anyway)"
        fi
    else
        # Has uncommitted changes, run on staged/unstaged files
        if pre-commit run --show-diff-on-failure --color=always; then
            print_success "Pre-commit checks passed"
        else
            print_warning "Pre-commit checks had issues (continuing anyway)"
        fi
    fi

    # Check requirements files
    echo "Checking requirements files..."
    if uv run tests/scripts/check_requirements.py; then
        print_success "Requirements files are valid"
    else
        print_error "Requirements files have issues"
        exit 1
    fi
fi

#
# INSTALL DEPENDENCIES
#
if [[ "$RUN_TESTS" == "true" ]]; then
    print_header "Installing Dependencies"

    # Build handler extras list
HANDLER_EXTRAS=()
for handler in "${HANDLERS_TO_INSTALL[@]}"; do
        HANDLER_EXTRAS+=(".[${handler}]")
    done

    echo "Installing mindsdb with handlers: ${HANDLERS_TO_INSTALL[*]}"
    uv pip install ".[agents,kb]" \
        -r requirements/requirements-test.txt \
        "${HANDLER_EXTRAS[@]}"

    # Install onnxruntime for ChromaDB
    echo "Installing onnxruntime..."
    uv pip install --force-reinstall onnxruntime==1.20.1

    # Clone parser tests
    PARSER_VERSION=$(uv pip show mindsdb_sql_parser | grep Version | cut -d ' ' -f 2)
    if [[ ! -d "parser_tests" ]]; then
        echo "Cloning mindsdb_sql_parser tests (v$PARSER_VERSION)..."
        git clone --branch "v$PARSER_VERSION" https://github.com/mindsdb/mindsdb_sql_parser.git parser_tests
    else
        print_success "parser_tests already exists"
    fi

    print_success "Dependencies installed"

    #
    # RUN UNIT TESTS
#
print_header "Running Unit Tests"

mkdir -p reports

if [[ "$RUN_SLOW" == "true" ]]; then
    echo "Running unit tests (with slow tests)..."
    make unit_tests_slow
else
    echo "Running unit tests (fast mode)..."
    make unit_tests
fi

    print_success "Unit tests completed"

    #
    # COVERAGE REPORT
    #
    print_header "Coverage Report"

    # Generate handler coverage report
    echo "Generating handler coverage report..."
    if uv run tests/scripts/check_handler_coverage.py > pytest-coverage-handlers.txt 2>/dev/null; then
        print_success "Handler coverage report: pytest-coverage-handlers.txt"
    else
        print_warning "Could not generate handler coverage report"
    fi

    # Generate HTML coverage report
    echo "Generating HTML coverage report..."
    if COVERAGE_FILE=.coverage.unit uv run coverage html -d reports/htmlcov 2>/dev/null; then
        print_success "HTML coverage report: reports/htmlcov/index.html"
        
        # Open in browser on macOS
        if [[ "$(uname)" == "Darwin" ]]; then
            echo "Opening coverage report in browser..."
            open reports/htmlcov/index.html
        fi
    else
        print_warning "Could not generate HTML coverage report"
    fi
fi

print_header "Done!"
echo "Summary:"
[[ "$RUN_CHECKS" == "true" ]] && echo "  - Static code checks: completed"
[[ "$RUN_TESTS" == "true" ]] && echo "  - Unit tests: completed"
echo ""
echo "Generated files:"
[[ -f "pytest.xml" ]] && echo "  - pytest.xml (JUnit test results)"
[[ -f "coverage.xml" ]] && echo "  - coverage.xml (Coverage XML)"
[[ -f "pytest-coverage.txt" ]] && echo "  - pytest-coverage.txt (Coverage summary)"
[[ -f "pytest-coverage-handlers.txt" ]] && echo "  - pytest-coverage-handlers.txt (Handler coverage)"
[[ -d "reports/htmlcov" ]] && echo "  - reports/htmlcov/index.html (HTML coverage report)"
