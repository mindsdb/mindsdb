import io
import os
import uuid
from textwrap import dedent

import pytest
import requests
from filelock import FileLock

from mindsdb.utilities.constants import DEFAULT_COMPANY_ID, DEFAULT_USER_ID

int_url_split = os.environ.get("INTERNAL_URL", "localhost").split(":")

INTERNAL_URL = int_url_split[0]
if len(int_url_split) == 1:
    HTTP_PORT = "80" if "svc.cluster.local" in INTERNAL_URL else "47334"
else:
    HTTP_PORT = int_url_split[1]

HTTP_API_ROOT = f"http://{INTERNAL_URL}:{HTTP_PORT}/api"
MYSQL_API_ROOT = INTERNAL_URL

lock = FileLock("train_finetune.lock")

# Generate unique test session ID to avoid conflicts between test runs
TEST_SESSION_ID = os.environ.get("TEST_SESSION_ID", uuid.uuid4().hex[:8])


def get_test_resource_name(base_name: str) -> str:
    """Generate a unique resource name for this test session."""
    return f"{base_name}_{TEST_SESSION_ID}"


def get_test_company_id(base_id: int = 1) -> str:
    """Generate a unique company ID for this test session.

    Uses underscores instead of hyphens to be SQL-safe when MindsDB
    uses these IDs internally for table names or storage paths.
    """
    return f"{TEST_SESSION_ID}_0000_0000_0000_{base_id:012d}"


def get_test_user_id(base_id: int = 1) -> str:
    """Generate a unique user ID for this test session.

    Uses underscores instead of hyphens to be SQL-safe when MindsDB
    uses these IDs internally for table names or storage paths.
    """
    return f"{TEST_SESSION_ID}_0000_0000_0001_{base_id:012d}"


class TestResourceTracker:
    """
    Tracks resources created during tests for cleanup.
    Only tracks and cleans test-specific resources.

    IMPORTANT: We do NOT track or drop databases/integrations.
    We only clean up the specific rows/resources we created:
    - Models, Views, ML Engines, Knowledge Bases, Tabs, Files
    """

    def __init__(self):
        self.models = set()  # (project_name, model_name) tuples
        self.views = set()  # (project_name, view_name) tuples
        self.ml_engines = set()  # ML engine names
        self.knowledge_bases = set()  # KB names
        self.tabs = []  # (company_id, user_id, tab_id) tuples
        self.files = set()  # File names

    def track_model(self, model_name: str, project_name: str = "mindsdb"):
        """Track a model for cleanup."""
        self.models.add((project_name, model_name))

    def track_view(self, view_name: str, project_name: str = "mindsdb"):
        """Track a view for cleanup."""
        self.views.add((project_name, view_name))

    def track_ml_engine(self, name: str):
        """Track an ML engine for cleanup."""
        self.ml_engines.add(name)

    def track_knowledge_base(self, name: str):
        """Track a knowledge base for cleanup."""
        self.knowledge_bases.add(name)

    def track_tab(self, company_id: str, user_id: str, tab_id: int):
        """Track a tab for cleanup."""
        self.tabs.append((company_id, user_id, tab_id))

    def track_file(self, name: str):
        """Track a file for cleanup."""
        self.files.add(name)


# Global tracker instance
_resource_tracker = TestResourceTracker()


def get_resource_tracker() -> TestResourceTracker:
    """Get the global resource tracker."""
    return _resource_tracker


@pytest.fixture(scope="session")
def test_session_id():
    """Unique identifier for this test session."""
    return TEST_SESSION_ID


@pytest.fixture(scope="session")
def resource_tracker():
    """Resource tracker for cleanup."""
    return _resource_tracker


@pytest.fixture(scope="session")
def train_finetune_lock():
    """
    Fixture to lock the training and fine-tuning process for the session.
    Because mindsdb can't have multiple models training/fine-tuning at the same time,
    """
    return lock


def _execute_cleanup_sql(sql: str, company_id: str = DEFAULT_COMPANY_ID, user_id: str = DEFAULT_USER_ID):
    """Execute a SQL statement for cleanup, ignoring errors."""
    try:
        headers = {"company-id": company_id, "user-id": user_id}
        payload = {"query": sql, "context": {}}
        response = requests.post(f"{HTTP_API_ROOT}/sql/query", json=payload, headers=headers, timeout=30)
        return response.status_code == 200
    except Exception:
        return False


def _cleanup_resources(tracker: TestResourceTracker):
    """
    Clean up all tracked resources.

    Cleanup order respects dependencies:
    1. Tabs - independent user data
    2. Files - independent uploaded files
    3. Knowledge bases - self-contained
    4. Views - may reference data from integrations
    5. Models - depend on ML engines (must delete before ML engines)
    6. ML engines - used by models (delete after models)

    NOTE: We do NOT drop databases/integrations - only the specific
    resources (rows) we created during tests.
    """
    # 1. Clean up tabs (independent)
    for company_id, user_id, tab_id in tracker.tabs:
        try:
            headers = {"company-id": company_id, "user-id": user_id}
            requests.delete(f"{HTTP_API_ROOT}/tabs/{tab_id}", headers=headers, timeout=10)
        except Exception:
            pass

    # 2. Clean up files (independent)
    for file_name in tracker.files:
        try:
            requests.delete(f"{HTTP_API_ROOT}/files/{file_name}", timeout=10)
        except Exception:
            pass

    # 3. Clean up knowledge bases (self-contained)
    # Use backticks to quote identifiers in case they contain special chars
    for kb_name in tracker.knowledge_bases:
        _execute_cleanup_sql(f"DROP KNOWLEDGE BASE IF EXISTS `{kb_name}`")

    # 4. Clean up views (reference data, not models)
    for project_name, view_name in tracker.views:
        _execute_cleanup_sql(f"DROP VIEW IF EXISTS `{project_name}`.`{view_name}`")

    # 5. Clean up models (depend on ML engines - must delete BEFORE ML engines)
    for project_name, model_name in tracker.models:
        _execute_cleanup_sql(f"DROP MODEL IF EXISTS `{project_name}`.`{model_name}`")

    # 6. Clean up ML engines (used by models - delete AFTER models)
    for engine_name in tracker.ml_engines:
        _execute_cleanup_sql(f"DROP ML_ENGINE IF EXISTS `{engine_name}`")


@pytest.fixture(scope="session", autouse=True)
def session_cleanup(resource_tracker):
    """
    Session-scoped fixture that cleans up test resources after all tests complete.
    This runs automatically at the end of the test session.
    """
    # Yield to run all tests first
    yield

    # Cleanup after all tests
    _cleanup_resources(resource_tracker)


def create_byom(name: str, target_column: str = "test", company_id = None, user_id = None):
    headers = {}
    if company_id is not None:
        headers["company-id"] = str(company_id)
    if user_id is not None:
        headers["user-id"] = str(user_id)

    def get_file():
        return io.BytesIO(
            dedent(f"""
                import pandas as pd
                class CustomPredictor():
                    def train(self, df, target_column, args=None):
                        pass
                    def predict(self, df, *args, **kwargs):
                        return pd.DataFrame([[1]], columns=['{target_column}'])
                    def describe(self, model_state, attribute):
                        return 'x'
            """).encode()
        )

    response = requests.put(
        f"{HTTP_API_ROOT}/handlers/byom/{name}",
        files={
            "code": ("test.py", get_file(), "text/x-python"),
        },
        data={
            "type": "inhouse",
        },
        headers=headers,
    )
    if response.status_code not in (200, 409):
        raise Exception("Error creating BYOM engine")