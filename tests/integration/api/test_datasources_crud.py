import os
import pytest
from http import HTTPStatus

# Get test database credentials from environment variables, with defaults to the public MindsDB database
db_user = os.getenv("TEST_DB_USER", "demo_user")
db_password = os.getenv("TEST_DB_PASSWORD", "demo_password")
db_host = os.getenv("TEST_DB_HOST", "samples.mindsdb.com")
db_port = os.getenv("TEST_DB_PORT", "5432")
db_name = os.getenv("TEST_DB_NAME", "demo")
db_schema = os.getenv("TEST_DB_SCHEMA", "demo_data")


class TestDatabasesAPI:
    """
    Test suite for CRUD operations on the /api/databases endpoints.
    """

    def test_create_postgres_datasource(self, client):
        """
        Tests the successful creation of a new Postgres datasource.
        """
        data = {
            "database": {
                "name": "test_postgres_crud",
                "engine": "postgres",
                "parameters": {
                    "user": db_user,
                    "password": db_password,
                    "host": db_host,
                    "port": db_port,
                    "database": db_name,
                    "schema": db_schema
                }
            }
        }
        response = client.post("/api/databases", json=data, follow_redirects=True)
        assert response.status_code == HTTPStatus.CREATED, response.text
        response_data = response.get_json()
        assert response_data["name"] == "test_postgres_crud"
        assert response_data["engine"] == "postgres"

    def test_create_datasource_already_exists(self, client):
        """
        Tests creating a datasource that already exists.
        """
        data = {
            "database": {
                "name": "test_postgres_crud_exists",
                "engine": "postgres",
                "parameters": {
                    "user": db_user,
                    "password": db_password,
                    "host": db_host,
                    "port": db_port,
                    "database": db_name,
                    "schema": db_schema
                }
            }
        }
        # Create it once
        response = client.post("/api/databases", json=data, follow_redirects=True)
        assert response.status_code == HTTPStatus.CREATED, response.text

        # Try to create it again
        response_conflict = client.post("/api/databases", json=data, follow_redirects=True)
        assert response_conflict.status_code == HTTPStatus.CONFLICT

    @pytest.mark.parametrize("payload", [
        {"database": {"engine": "postgres", "parameters": {}}},  # Missing name
        {"database": {"name": "test_missing_engine", "parameters": {}}},  # Missing engine
    ])
    def test_create_datasource_missing_required_fields(self, client, payload):
        """
        Tests creating a datasource with missing name or engine in the payload.
        """
        response = client.post("/api/databases", json=payload, follow_redirects=True)
        assert response.status_code == HTTPStatus.BAD_REQUEST

    def test_list_all_databases(self, client):
        """
        Tests listing all databases.
        """
        response = client.get("/api/databases", follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert isinstance(response_data, list)
        db_names = {db["name"] for db in response_data}
        assert "mindsdb" in db_names
        assert "information_schema" in db_names

    def test_get_existing_database(self, client):
        """
        Tests fetching a specific, existing database by name.
        """
        response = client.get("/api/databases/mindsdb", follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert response_data["name"] == "mindsdb"

    def test_get_non_existent_database(self, client):
        """
        Tests fetching a non-existent database.
        """
        response = client.get("/api/databases/non_existent_db_abc", follow_redirects=True)
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_update_existing_database(self, client):
        """
        Tests updating the connection parameters of an existing database.
        """
        db_name = "test_db_to_update"
        create_data = {
            "database": {
                "name": db_name,
                "engine": "postgres",
                "parameters": {"user": "old_user"}
            }
        }
        client.post("/api/databases", json=create_data, follow_redirects=True)

        update_data = {
            "database": {
                "parameters": {"user": "new_user", "password": "new_password"}
            }
        }
        response = client.put(f"/api/databases/{db_name}", json=update_data, follow_redirects=True)
        assert response.status_code == HTTPStatus.OK
        response_data = response.get_json()
        assert response_data["connection_data"]["user"] == "new_user"
        assert response_data["connection_data"]["password"] == "new_password"

    def test_update_non_existent_database_creates_it(self, client):
        """
        Tests that using PUT on a non-existent database name creates it.
        """
        data = {
            "database": {
                "name": "test_put_will_create",
                "engine": "postgres",
                "parameters": {"user": "some_user"}
            }
        }
        response = client.put("/api/databases/test_put_will_create", json=data, follow_redirects=True)
        assert response.status_code == HTTPStatus.CREATED

    def test_delete_database(self, client):
        """
        Tests the successful deletion of a database and verifies it's gone.
        """
        db_name_to_delete = "test_db_to_delete"
        data = {
            "database": {
                "name": db_name_to_delete,
                "engine": "postgres",
                "parameters": {}
            }
        }
        client.post("/api/databases", json=data, follow_redirects=True)

        # Delete the database
        delete_response = client.delete(f"/api/databases/{db_name_to_delete}", follow_redirects=True)
        assert delete_response.status_code == HTTPStatus.NO_CONTENT

        # Verify it no longer exists
        get_response = client.get(f"/api/databases/{db_name_to_delete}", follow_redirects=True)
        assert get_response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_non_existent_database(self, client):
        """
        Tests deleting a non-existent database.
        """
        response = client.delete("/api/databases/non_existent_db_xyz", follow_redirects=True)
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.parametrize("system_db", ["mindsdb", "information_schema", "files"])
    def test_delete_system_database_not_allowed(self, client, system_db):
        """
        Tests that attempting to delete a system database is not allowed.
        """
        response = client.delete(f"/api/databases/{system_db}", follow_redirects=True)
        if system_db == "files":
            # 'files' is a special project and returns 404 instead of 400
            assert response.status_code == HTTPStatus.NOT_FOUND
        else:
            assert response.status_code == HTTPStatus.BAD_REQUEST