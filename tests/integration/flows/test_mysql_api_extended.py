import os
import pytest
import uuid
import time
from .test_mysql_api import BaseStuff
import mysql.connector


@pytest.fixture(scope="module")
def setup_local_db():
    """Module-scoped fixture to create a writeable DB for table tests."""
    db_name = f"test_db_{uuid.uuid4().hex[:8]}"
    helper = BaseStuff()
    helper.use_binary = False

    params = {"user": "postgres", "password": "postgres", "host": "postgres", "port": 5432, "database": "postgres"}

    print(f"\n--> [Fixture setup_local_db] Setting up local database: {db_name} on {params['host']}:{params['port']}")
    try:
        helper.query(f"DROP DATABASE IF EXISTS {db_name}")
        create_datasource_sql_via_connector(helper, db_name, "postgres", params)
        yield db_name
    except mysql.connector.Error as e:
        pytest.skip(
            f"\n\n--- FIXTURE SETUP FAILED ---\n"
            f"Could not connect to the PostgreSQL container ('{params['host']}').\n"
            f"Please ensure your Docker Compose environment is running correctly.\n"
            f"Original Error: {e}\n"
        )
    finally:
        print(f"\n--> [Fixture setup_local_db] Tearing down database: {db_name}")
        try:
            helper.query(f"DROP DATABASE IF EXISTS {db_name};")
        except mysql.connector.Error:
            pass


def create_datasource_sql_via_connector(helper_instance, db_name, engine, parameters, poll_timeout=30, poll_interval=2):
    """Helper to create a datasource via a CREATE DATABASE query."""
    params_list = [f'"{k}": "{v}"' if isinstance(v, str) else f'"{k}": {v}' for k, v in parameters.items()]
    params_str = ", ".join(params_list)
    query_str = f"CREATE DATABASE {db_name} WITH ENGINE = '{engine}', PARAMETERS = {{{params_str}}};"
    print(f"    [Helper create_datasource] Executing: CREATE DATABASE {db_name}...")
    helper_instance.query(query_str)
    start_time = time.time()
    while True:
        try:
            helper_instance.validate_database_creation(db_name)
            print(f"     [Helper create_datasource] DATABASE {db_name} created and validated.")
            break
        except AssertionError as e:
            elapsed_time = time.time() - start_time
            if elapsed_time > poll_timeout:
                print(f"     [Helper create_datasource] ERROR: Timeout after {poll_timeout}s waiting for {db_name}.")
                raise TimeoutError(f"Timed out waiting for database {db_name} to be created.") from e
            time.sleep(poll_interval)


def wait_for_trigger(self, db_name, trigger_name, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        triggers = self.query(f"SHOW TRIGGERS FROM {db_name};")
        if any(t["trigger_name"] == trigger_name for t in triggers):
            print(f"[DEBUG] Trigger {trigger_name} registered after {time.time() - start:.2f}s")
            return True
        time.sleep(2)
    raise TimeoutError(f"Trigger {trigger_name} not found after {timeout}s")


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLTables(BaseStuff):
    """Test suite for Table operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("setup_local_db")
    def test_table_lifecycle(self, setup_local_db, use_binary):
        db_name = setup_local_db
        table_name = "test_lifecycle_table"
        try:
            create_table_query = f"CREATE TABLE {db_name}.{table_name} (id INT, value VARCHAR(255));"
            self.query(create_table_query)
            result = self.query(f"SHOW TABLES FROM {db_name};")
            assert table_name in [row["Tables_in_" + db_name] for row in result]
            replace_query = f"CREATE OR REPLACE TABLE {db_name}.{table_name} (SELECT 2 as id, 'new_data' as value);"
            self.query(replace_query)
            result = self.query(f"SELECT * FROM {db_name}.{table_name};")
            assert result and result[0]["id"] == 2 and result[0]["value"] == "new_data"
        finally:
            self.query(f"DROP TABLE IF EXISTS {db_name}.{table_name};")


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLTablesNegative(BaseStuff):
    """Negative tests for Table operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("setup_local_db")
    def test_create_duplicate_table(self, setup_local_db, use_binary):
        db_name = setup_local_db
        table_name = "test_duplicate_table"
        create_query = f"CREATE TABLE {db_name}.{table_name} (id INT);"
        try:
            self.query(create_query)
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            self.query(f"DROP TABLE IF EXISTS {db_name}.{table_name};")

    def test_create_table_in_missing_db_raises_error(self, use_binary):
        create_query = "CREATE TABLE non_existent_db.non_existent_table (id INT);"
        with pytest.raises(Exception) as e:
            self.query(create_query)
        assert "Database not found" in str(e.value).lower()

    @pytest.mark.usefixtures("setup_local_db")
    def test_drop_non_existent_table(self, setup_local_db, use_binary):
        db_name = setup_local_db
        table_name = "test_non_existent_table"
        with pytest.raises(Exception) as e:
            self.query(f"DROP TABLE {db_name}.{table_name};")
        assert "does not exist" in str(e.value).lower()


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLViews(BaseStuff):
    """Test suite for View operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    def test_view_lifecycle(self, use_binary):
        db_name = "test_sql_view_db"
        view_name = "test_sql_view"
        try:
            create_db_query = f"""
                CREATE DATABASE {db_name}
                WITH ENGINE = 'postgres', PARAMETERS = {{"user": "demo_user", "password": "demo_password", "host": "samples.mindsdb.com", "port": "5432", "database": "demo", "schema": "demo"}};
            """
            self.query(create_db_query)

            create_view_query = (
                f"CREATE VIEW {view_name} AS (SELECT * FROM {db_name}.home_rentals WHERE number_of_rooms = 2);"
            )
            self.query(create_view_query)
            result = self.query("SHOW VIEWS;")
            assert view_name in [row.get("Name", row.get("NAME")) for row in result]
            result = self.query(f"SELECT * FROM {view_name};")
            assert len(result) > 0 and all(row["number_of_rooms"] == 2 for row in result)
            alter_view_query = (
                f"ALTER VIEW {view_name} AS (SELECT * FROM {db_name}.home_rentals WHERE number_of_rooms = 1);"
            )
            self.query(alter_view_query)
            result_after_alter = self.query(f"SELECT * FROM {view_name};")
            assert len(result_after_alter) > 0 and all(row["number_of_rooms"] == 1 for row in result_after_alter)
        finally:
            self.query(f"DROP VIEW IF EXISTS {view_name};")
            self.query(f"DROP DATABASE IF EXISTS {db_name};")


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLViewsNegative(BaseStuff):
    """Negative tests for View operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    def test_create_duplicate_view(self, use_binary):
        view_name = "test_duplicate_view"
        create_query = f"CREATE VIEW {view_name} AS (SELECT 1);"
        try:
            self.query(create_query)
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            self.query(f"DROP VIEW IF EXISTS {view_name};")

    def test_create_view_on_non_existent_table(self, use_binary):
        view_name = "test_bad_source_view"
        create_query = f"CREATE VIEW {view_name} AS (SELECT * FROM non_existent_db.non_existent_table);"
        with pytest.raises(Exception) as e:
            self.query(create_query)
        error_str = str(e.value).lower()
        assert "not found in the database" in error_str or "table name should contain only one part" in error_str

    def test_drop_non_existent_view(self, use_binary):
        view_name = "non_existent_view"
        with pytest.raises(Exception) as e:
            self.query(f"DROP VIEW {view_name};")
        error_str = str(e.value).lower()
        assert "view not found" in error_str or "unknown view" in error_str


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLKnowledgeBases(BaseStuff):
    """Test suite for Knowledge Base operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    def test_knowledge_base_full_lifecycle(self, use_binary):
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY environment variable not set. Skipping Knowledge Base lifecycle test.")

        kb_name = "test_kb_sql"
        content_to_insert = "MindsDB helps developers build AI-powered applications."
        embedding_model = "text-embedding-3-small"
        try:
            create_kb_query = f"""
                CREATE KNOWLEDGE_BASE {kb_name}
                USING embedding_model = {{"provider": "openai", "model_name": "{embedding_model}", "api_key": "{openai_api_key}"}};
            """
            self.query(create_kb_query)
            result = self.query(f"DESCRIBE KNOWLEDGE_BASE {kb_name};")
            assert result and result[0]["NAME"] == kb_name and embedding_model in result[0]["EMBEDDING_MODEL"]
            self.query(f"INSERT INTO {kb_name} (content) VALUES ('{content_to_insert}');")
            result = self.query(f"SELECT chunk_content FROM {kb_name} WHERE content = 'What is MindsDB?';")
            assert result and "MindsDB" in result[0]["chunk_content"]
        finally:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")

    def test_create_kb_with_invalid_provider(self, use_binary):
        kb_name = "test_invalid_provider"
        create_query = (
            f'CREATE KNOWLEDGE_BASE {kb_name} USING embedding_model = {{"provider": "non_existent_provider"}};'
        )
        with pytest.raises(Exception) as e:
            self.query(create_query)
        assert "wrong embedding provider" in str(e.value).lower()

    def test_create_kb_with_invalid_api_key(self, use_binary):
        kb_name = f"test_invalid_key_{uuid.uuid4().hex[:8]}"
        create_query = f'CREATE KNOWLEDGE_BASE {kb_name} USING embedding_model = {{"provider": "openai", "api_key": "this_is_a_fake_key"}};'
        with pytest.raises(Exception) as e:
            self.query(create_query)
        assert (
            "problem with embedding model config" in str(e.value).lower() or "invalid api key" in str(e.value).lower()
        )

    def test_insert_into_non_existent_kb(self, use_binary):
        kb_name = "non_existent_kb"
        with pytest.raises(Exception) as e:
            self.query(f"INSERT INTO {kb_name} (content) VALUES ('some data');")
        error_str = str(e.value).lower()
        assert "can't create table" in error_str or "doesn't exist" in error_str or "unknown table" in error_str

    def test_query_non_existent_kb(self, use_binary):
        kb_name = "non_existent_kb"
        with pytest.raises(Exception) as e:
            self.query(f"SELECT * FROM {kb_name} WHERE content = 'some query';")
        error_str = str(e.value).lower()
        assert "not found in database" in error_str or "doesn't exist" in error_str or "unknown table" in error_str

    def test_create_duplicate_kb(self, use_binary):
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY environment variable not set. Skipping duplicate KB test.")

        kb_name = "test_duplicate_kb"
        embedding_model = "text-embedding-3-small"
        create_query = f"""
            CREATE KNOWLEDGE_BASE {kb_name}
            USING embedding_model = {{"provider": "openai", "model_name": "{embedding_model}", "api_key": "{openai_api_key}"}};
        """
        try:
            self.query(create_query)
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")


@pytest.fixture(scope="function")
def setup_trigger_db():
    """Function-scoped fixture to ensure a clean DB for each trigger test."""
    db_name = f"trigger_test_db_{uuid.uuid4().hex[:8]}"
    params = {"user": "postgres", "password": "postgres", "host": "postgres", "port": 5432, "database": "postgres"}
    source_table_name = f"source_table_{uuid.uuid4().hex[:8]}"
    target_table_name = f"target_table_{uuid.uuid4().hex[:8]}"
    helper = BaseStuff()
    helper.use_binary = False
    try:
        print(
            f"\n--> [Fixture setup_trigger_db] Setting up local database: {db_name} on {params['host']}:{params['port']}"
        )
        helper.query(f"DROP DATABASE IF EXISTS {db_name}")
        create_datasource_sql_via_connector(helper, db_name, "postgres", params)

        helper.query(f"CREATE TABLE {db_name}.{source_table_name} (id INT, message VARCHAR(255));")
        helper.query(f"CREATE TABLE {db_name}.{target_table_name} (id INT, message VARCHAR(255));")
        helper.query(f"INSERT INTO {db_name}.{source_table_name} (id, message) VALUES (101, 'initial_update_message');")
        helper.query(f"INSERT INTO {db_name}.{source_table_name} (id, message) VALUES (102, 'initial_delete_message');")
        yield db_name, source_table_name, target_table_name
    except mysql.connector.Error as setup_err:
        pytest.fail(f"Trigger fixture setup failed. Ensure Docker environment is running. Error: {setup_err}")
    finally:
        print(f"\n--> [CLEANUP] Dropping DATABASE: {db_name}")
        try:
            helper.query(f"DROP DATABASE IF EXISTS {db_name};")
        except mysql.connector.Error:
            pass


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLTriggers(BaseStuff):
    """Test suite for Trigger operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("setup_trigger_db")
    def test_trigger_lifecycle_update(self, setup_trigger_db, use_binary):
        db_name, source_table_name, target_table_name = setup_trigger_db
        trigger_name = "test_update_trigger"
        test_id = 101
        updated_message = "this message was updated"
        try:
            # Ensure the target table is empty before each test run.
            self.query(f"DELETE FROM {db_name}.{target_table_name};")

            create_trigger_query = f"""
                CREATE TRIGGER {trigger_name}
                ON {db_name}.{source_table_name}
                (INSERT INTO {db_name}.{target_table_name} (id, message) SELECT id, message FROM TABLE_DELTA);
            """
            self.query(create_trigger_query)
            time.sleep(5)  # Allow time for trigger creation

            # Activate Trigger
            self.query(f"UPDATE {db_name}.{source_table_name} SET message = '{updated_message}' WHERE id = {test_id};")

            # Poll the target table for the result
            result = []
            max_wait_time = 60
            interval = 1
            max_interval = 8
            start_time = time.time()
            result = []
            while time.time() - start_time < max_wait_time:
                result = self.query(f"SELECT id, message FROM {db_name}.{target_table_name} WHERE id = {test_id};")
                if result:
                    break
                time.sleep(interval)
                interval = min(interval * 2, max_interval)

            # Verify
            assert result, f"Trigger did not fire for id {test_id} within {max_wait_time}s."
            assert result[0]["message"] == updated_message
        finally:
            self.query(f"DROP TRIGGER {trigger_name};")


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLTriggersNegative(BaseStuff):
    """Negative tests for Trigger operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("setup_trigger_db")
    def test_create_duplicate_trigger(self, setup_trigger_db, use_binary):
        db_name, source_table_name, _ = setup_trigger_db
        trigger_name = "duplicate_trigger"
        create_query = f"CREATE TRIGGER {trigger_name} ON {db_name}.{source_table_name} (SELECT 1);"
        try:
            self.query(create_query)
            time.sleep(2)
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            self.query(f"DROP TRIGGER {trigger_name};")

    def test_create_trigger_on_non_existent_table(self, use_binary):
        trigger_name = f"bad_trigger_{uuid.uuid4().hex[:8]}"
        create_query = f"CREATE TRIGGER {trigger_name} ON non_existent_db.non_existent_table (SELECT 1);"
        with pytest.raises(Exception) as e:
            self.query(create_query)
        error_str = str(e.value).lower()
        assert "no integration with name" in error_str or "unknown database" in error_str

    def test_drop_non_existent_trigger(self, use_binary):
        trigger_name = "non_existent_trigger"
        with pytest.raises(Exception) as e:
            self.query(f"DROP TRIGGER {trigger_name};")
        error_str = str(e.value).lower()
        assert "doesn't exist" in error_str or "unknown trigger" in error_str
