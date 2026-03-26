#!/usr/bin/env bash
#
# Local Integration Test Runner for macOS
# Replicates the GitHub Actions workflow from .github/workflows/tests_integration.yml
#
# Usage:
#   ./run_integration_tests.sh                    # Run all integration tests (slow)
#   ./run_integration_tests.sh --fast             # Run tests without --runslow flag
#   ./run_integration_tests.sh --help             # Show this help
#
# Required environment variables:
#   OPENAI_API_KEY    - OpenAI API key for tests that need it
#   INTERNAL_URL      - MindsDB internal URL (optional, for deploy tests)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
RUN_SLOW=true
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"

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
    echo "Local Integration Test Runner for macOS"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --fast          Run tests without --runslow flag (faster)"
    echo "  --help          Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  PYTHON_VERSION  Python version to use (default: 3.11)"
    echo "  OPENAI_API_KEY  OpenAI API key (required for some tests)"
    echo "  INTERNAL_URL    MindsDB internal URL (optional, for deploy tests)"
    echo ""
    echo "Examples:"
    echo "  $0                              # Run all integration tests"
    echo "  $0 --fast                       # Run fast integration tests only"
    echo "  OPENAI_API_KEY=sk-xxx $0        # Run with OpenAI key"
    echo ""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
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

print_header "MindsDB Integration Test Runner"

# Check for required environment variables
if [[ -z "$OPENAI_API_KEY" ]]; then
    print_warning "OPENAI_API_KEY is not set. Some tests may be skipped or fail."
    echo "  Set it with: export OPENAI_API_KEY=sk-your-key"
fi

if [[ -n "$INTERNAL_URL" ]]; then
    print_success "INTERNAL_URL is set: $INTERNAL_URL"
else
    print_warning "INTERNAL_URL is not set. Deploy-specific tests will be skipped."
fi

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
# INSTALL DEPENDENCIES
#
print_header "Installing Dependencies"

echo "Installing test requirements..."
uv pip install -r requirements/requirements-test.txt

# Also install mindsdb itself for integration tests
echo "Installing mindsdb..."
uv pip install -e .

print_success "Dependencies installed"

#
# RUN INTEGRATION TESTS
#
print_header "Running Integration Tests"

mkdir -p reports

if [[ "$RUN_SLOW" == "true" ]]; then
    echo "Running integration tests (with slow tests)..."
    make integration_tests_slow
else
    echo "Running integration tests (fast mode)..."
    make integration_tests
fi

print_success "Integration tests completed"

print_header "Done!"
echo "Summary:"
echo "  - Integration tests: completed"
echo ""
if [[ "$RUN_SLOW" == "true" ]]; then
    echo "Ran with --runslow flag (full test suite)"
else
    echo "Ran in fast mode (skipped slow tests)"
fi
