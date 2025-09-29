import os
import pytest
import uuid
from http import HTTPStatus

# --- Test Configuration ---
db_user = os.getenv("TEST_DB_USER", "demo_user")
db_password = os.getenv("TEST_DB_PASSWORD", "demo_password")
db_host = os.getenv("TEST_DB_HOST", "samples.mindsdb.com")
db_port = os.getenv("TEST_DB_PORT", "5432")
db_name = os.getenv("TEST_DB_NAME", "demo")
db_schema = os.getenv("TEST_DB_SCHEMA", "demo_data")


# --- Helper Fixture for Test Isolation ---
@pytest.fixture
def managed_db(api_client):
    """
    A fixture that creates a database for a test and guarantees its deletion afterward.
    This is the key to ensuring test isolation and preventing 409 errors.
    """
    created_db_names = []

    def _create_and_manage_db(name, engine="postgres", params=None):
        """Creates a database and registers it for cleanup."""
        # Use a unique name to prevent collisions between test runs
        unique_name = f"{name}_{uuid.uuid4().hex[:8]}"

        payload = {
            "database": {
                "name": unique_name,
                "engine": engine,
                "parameters": params or {},
            }
        }

        # In case a previous teardown failed, ensure we start clean.
        api_client.delete(f"{api_client.base_url}/api/databases/{unique_name}")

        # Create the database for the test
        response = api_client.post(f"{api_client.base_url}/api/databases", json=payload)
        assert response.status_code == HTTPStatus.CREATED, (
            f"Setup failed: Could not create database '{unique_name}'. Response: {response.text}"
        )

        created_db_names.append(unique_name)
        return unique_name, response.json()

    # Yield the creator function to the test function
    yield _create_and_manage_db

    # --- Teardown ---
    # This code block is executed after the test function that uses this fixture completes
    for db_name_to_delete in created_db_names:
        try:
            api_client.delete(f"{api_client.base_url}/api/databases/{db_name_to_delete}")
        except Exception as e:
            print(f"Warning: Teardown failed to delete database '{db_name_to_delete}'. Error: {e}")


class TestDatabasesAPI:
    """
    Test suite for CRUD operations on the /api/databases endpoints.
    """

    def test_create_postgres_datasource(self, api_client):
        """
        Tests the successful creation of a new Postgres datasource.
        """
        # Use a unique name to ensure this test is self-contained and repeatable
        db_name = f"test_create_pg_{uuid.uuid4().hex[:8]}"
        data = {
            "database": {
                "name": db_name,
                "engine": "postgres",
                "parameters": {
                    "user": db_user,
                    "password": db_password,
                    "host": db_host,
                    "port": db_port,
                    "database": db_name,
                    "schema": db_schema,
                },
            }
        }
        try:
            # ACT
            response = api_client.post(f"{api_client.base_url}/api/databases", json=data)

            # ASSERT
            assert response.status_code == HTTPStatus.CREATED, response.text
            response_data = response.json()
            assert response_data["name"] == db_name
            assert response_data["engine"] == "postgres"
        finally:
            # CLEANUP: Ensure the created database is always deleted
            api_client.delete(f"{api_client.base_url}/api/databases/{db_name}")

    def test_create_datasource_already_exists(self, api_client, managed_db):
        """
        Tests creating a datasource that already exists returns a 409 Conflict.
        """
        # ARRANGE: Create a database that is automatically managed (created and deleted)
        db_name, _ = managed_db("test_conflict_db", params={"user": "test"})

        existing_db_payload = {"database": {"name": db_name, "engine": "postgres", "parameters": {}}}

        # ACT: Attempt to create the exact same database again
        response_conflict = api_client.post(f"{api_client.base_url}/api/databases", json=existing_db_payload)

        # ASSERT
        assert response_conflict.status_code == HTTPStatus.CONFLICT

    def test_update_existing_database(self, api_client, managed_db):
        """
        Tests updating an existing database's parameters.
        """
        # ARRANGE: Create a database to be updated
        db_name, _ = managed_db("test_db_to_update", params={"user": "old_user"})

        # ACT: Update the database with new parameters
        update_data = {"database": {"parameters": {"user": "new_user", "password": "new_password"}}}
        response = api_client.put(f"{api_client.base_url}/api/databases/{db_name}", json=update_data)

        # ASSERT
        assert response.status_code == HTTPStatus.OK
        response_data = response.json()
        assert response_data["connection_data"]["user"] == "new_user"

    def test_update_non_existent_database_creates_it(self, api_client):
        """
        Tests that a PUT request on a non-existent database name creates it (returns 201).
        """
        db_name = f"test_put_creates_{uuid.uuid4().hex[:8]}"
        data = {"database": {"name": db_name, "engine": "postgres", "parameters": {"user": "some_user"}}}

        try:
            # ARRANGE: Make sure it's gone before the test
            api_client.delete(f"{api_client.base_url}/api/databases/{db_name}")

            # ACT
            response = api_client.put(f"{api_client.base_url}/api/databases/{db_name}", json=data)

            # ASSERT: Per REST standards, a PUT that creates a new resource should return 201 CREATED
            assert response.status_code == HTTPStatus.CREATED
        finally:
            # CLEANUP
            api_client.delete(f"{api_client.base_url}/api/databases/{db_name}")

    def test_delete_database(self, api_client, managed_db):
        """
        Tests the successful deletion of a database.
        """
        # ARRANGE: Create a database to be deleted.
        db_name_to_delete, _ = managed_db("test_db_to_delete")

        # ACT
        delete_response = api_client.delete(f"{api_client.base_url}/api/databases/{db_name_to_delete}")

        # ASSERT
        assert delete_response.status_code == HTTPStatus.NO_CONTENT

        # VERIFY
        get_response = api_client.get(f"{api_client.base_url}/api/databases/{db_name_to_delete}")
        assert get_response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize(
        "payload",
        [
            {"database": {"engine": "postgres", "parameters": {}}},
            {"database": {"name": "test_missing_engine", "parameters": {}}},
        ],
    )
    def test_create_datasource_missing_required_fields(self, api_client, payload):
        response = api_client.post(f"{api_client.base_url}/api/databases", json=payload)
        assert response.status_code == HTTPStatus.BAD_REQUEST

    def test_list_all_databases(self, api_client):
        response = api_client.get(f"{api_client.base_url}/api/databases")
        assert response.status_code == HTTPStatus.OK
        response_data = response.json()
        assert isinstance(response_data, list)
        db_names = {db["name"] for db in response_data}
        assert "mindsdb" in db_names
        assert "information_schema" in db_names

    def test_get_existing_database(self, api_client):
        response = api_client.get(f"{api_client.base_url}/api/databases/mindsdb")
        assert response.status_code == HTTPStatus.OK
        response_data = response.json()
        assert response_data["name"] == "mindsdb"

    def test_get_non_existent_database(self, api_client):
        response = api_client.get(f"{api_client.base_url}/api/databases/non_existent_db_abc")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_non_existent_database(self, api_client):
        response = api_client.delete(f"{api_client.base_url}/api/databases/non_existent_db_xyz")
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize("system_db", ["mindsdb", "information_schema", "files"])
    def test_delete_system_database_not_allowed(self, api_client, system_db):
        response = api_client.delete(f"{api_client.base_url}/api/databases/{system_db}")
        assert 400 <= response.status_code < 500
