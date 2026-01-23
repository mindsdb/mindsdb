import os
import pytest
import time
from .test_mysql_api import BaseStuff
import mysql.connector


@pytest.fixture(scope="module")
def setup_local_db():
    """Module-scoped fixture to create a writeable DB for table tests."""
    db_name = "test_db_local"
    helper = BaseStuff()
    helper.use_binary = False

    params = {"user": "postgres", "password": "postgres", "host": "postgres", "port": 5432, "database": "postgres"}

    print(f"\n--> [Fixture setup_local_db] Setting up local database: {db_name} on {params['host']}:{params['port']}")
    try:
        helper.query(f"DROP DATABASE IF EXISTS {db_name}")
        create_datasource_sql_via_connector(helper, db_name, "postgres", params)
        yield db_name
    except (mysql.connector.Error, TimeoutError) as e:
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
    print(f"     [Helper create_datasource] Executing: CREATE DATABASE {db_name}...")
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


def wait_for_trigger_creation(query_fn, trigger_name, timeout=20, max_interval=5):
    """
    Polls information_schema to see if a trigger is visible.
    Does not raise an error, returns True (found) or False (not found).
    """
    start = time.time()
    interval = 1
    print(f"\n[DEBUG] Checking for trigger '{trigger_name}' in information_schema (timeout={timeout}s)...")
    while time.time() - start < timeout:
        try:
            result = query_fn(f"SELECT 1 FROM information_schema.triggers WHERE trigger_name = '{trigger_name}';")
            if result:
                print(f"[DEBUG] Trigger '{trigger_name}' found in information_schema after {time.time() - start:.2f}s.")
                return True
        except Exception:
            pass

        try:
            result = query_fn("SHOW TRIGGERS;")
            if result and trigger_name in [row.get("Trigger", row.get("TRIGGER")) for row in result]:
                print(f"[DEBUG] Trigger '{trigger_name}' found in SHOW TRIGGERS after {time.time() - start:.2f}s.")
                return True
        except Exception:
            pass
        time.sleep(interval)
        interval = min(interval * 1.5, max_interval)

    print(
        f"[DEBUG] WARNING: Trigger '{trigger_name}' was not found in metadata after {timeout}s. Proceeding with functional test..."
    )
    return False


def wait_for_trigger_to_fire(
    query_fn, db_name, source_table_name, target_table_name, test_id, updated_message, timeout=120, max_interval=10
):
    """
    Polls for a trigger to fire by repeatedly sending the UPDATE command
    and checking the target table. This is robust against trigger creation lag.
    """
    start = time.time()
    interval = 1

    while time.time() - start < timeout:
        print(f"[DEBUG] Firing trigger (elapsed={time.time() - start:.1f}s, interval={interval:.2f}s)...")
        query_fn(f"UPDATE {db_name}.{source_table_name} SET message = '{updated_message}' WHERE id = {test_id};")

        time.sleep(interval)

        result = query_fn(f"SELECT id, message FROM {db_name}.{target_table_name} WHERE id = {test_id};")
        if result:
            elapsed = time.time() - start
            print(f"[DEBUG] Trigger fired and verified after {elapsed:.2f}s → {result}")
            return result

        print("[DEBUG] Trigger not fired yet. Retrying...")
        interval = min(interval * 1.5, max_interval)

    raise TimeoutError(f"Trigger did not fire for id {test_id} within {timeout}s despite repeated attempts.")


def wait_for_kb_creation(query_fn, kb_name, timeout=90, poll_interval=1):
    """Polls to check if a Knowledge Base has been created successfully."""
    start_time = time.time()
    while True:
        try:
            result = query_fn(f"DESCRIBE KNOWLEDGE_BASE {kb_name};")
            if result and result[0]["name"] == kb_name:
                print(f"     [Helper wait_for_kb] KB {kb_name} created and validated.")
                return result
        except Exception:
            # KB might not be queryable at all yet
            pass

        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print(f"     [Helper wait_for_kb] ERROR: Timeout after {timeout}s waiting for {kb_name}.")
            raise TimeoutError(f"Timed out waiting for Knowledge Base {kb_name} to be created.")
        time.sleep(poll_interval)


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
            assert table_name in [list(row.values())[0] for row in result]
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
        assert "non_existent_db" or "Database not found" in str(e.value).lower()

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
            self.query(f"DROP VIEW IF EXISTS {view_name};")
            self.query(f"DROP DATABASE IF EXISTS {db_name};")

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
            assert view_name in [row.get("name", row.get("Name", row.get("NAME"))) for row in result]
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
            self.query(f"DROP VIEW IF EXISTS {view_name};")
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
        try:
            self.query(f"DROP VIEW IF EXISTS {view_name};")
        except Exception:
            pass

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

    @pytest.fixture
    def basic_kb(self, request):
        """
        Fixture to create a basic Knowledge Base for alteration tests.
        Requires OPENAI_API_KEY.
        """
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY environment variable not set. Skipping KB tests.")

        kb_name = "test_alter_kb_local"
        embedding_model = "text-embedding-3-small"

        create_kb_query = f"""
            CREATE KNOWLEDGE_BASE {kb_name}
            USING embedding_model = {{"provider": "openai", "model_name": "{embedding_model}", "api_key": "{openai_api_key}"}};
        """
        try:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")
            self.query(create_kb_query)
            result = wait_for_kb_creation(self.query, kb_name)
            assert result and result[0]["name"] == kb_name
            yield kb_name
        finally:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")

    def test_knowledge_base_full_lifecycle(self, use_binary):
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY environment variable not set. Skipping Knowledge Base lifecycle test.")

        kb_name = "test_kb_sql"
        content_to_insert = "MindsDB helps developers build AI-powered applications."
        embedding_model = "text-embedding-3-small"
        try:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")
            create_kb_query = f"""
                CREATE KNOWLEDGE_BASE {kb_name}
                USING embedding_model = {{"provider": "openai", "model_name": "{embedding_model}", "api_key": "{openai_api_key}"}};
            """
            self.query(create_kb_query)
            result = wait_for_kb_creation(self.query, kb_name)
            assert result and result[0]["name"] == kb_name and embedding_model in result[0]["embedding_model"]
            self.query(f"INSERT INTO {kb_name} (content) VALUES ('{content_to_insert}');")

            # Give insertion a moment to process before querying
            time.sleep(2)

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

    def test_create_kb_with_invalid_api_key(self, use_binary, request):
        kb_name = "test_invalid_key_local"
        create_query = f'CREATE KNOWLEDGE_BASE {kb_name} USING embedding_model = {{"provider": "openai", "api_key": "this_is_a_fake_key"}};'
        try:
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert (
                "problem with embedding model config" in str(e.value).lower()
                or "invalid api key" in str(e.value).lower()
            )
        finally:
            # Ensure cleanup even if creation fails
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")

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

    def test_create_duplicate_kb(self, use_binary, request):
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
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")
            self.query(create_query)
            wait_for_kb_creation(self.query, kb_name)
            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            self.query(f"DROP KNOWLEDGE_BASE IF EXISTS {kb_name};")

    @pytest.mark.usefixtures("basic_kb")
    def test_alter_kb_embedding_api_key(self, basic_kb, use_binary):
        """Tests altering the api_key of the embedding_model."""
        kb_name = basic_kb
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY needed for this alter test.")

        new_api_key = openai_api_key

        alter_query = f"""
            ALTER KNOWLEDGE_BASE {kb_name}
            USING
                embedding_model = {{ 'api_key': '{new_api_key}' }};
        """
        self.query(alter_query)

        time.sleep(1)

        result = self.query(f"SELECT embedding_model FROM information_schema.knowledge_bases WHERE name = '{kb_name}';")
        assert result
        embedding_model_json = result[0].get("embedding_model")
        assert embedding_model_json is not None

        assert '"provider": "openai"' in embedding_model_json
        assert '"model_name": "text-embedding-3-small"' in embedding_model_json
        assert '"api_key": "' in embedding_model_json

    @pytest.mark.xfail(
        reason="Bug: ALTER KNOWLEDGE_BASE does not unset reranking_model. See LINEAR-TICKET-NUMBER: FQE-1716"
    )
    @pytest.mark.usefixtures("basic_kb")
    def test_alter_kb_reranking_model(self, basic_kb, use_binary):
        """Tests adding and then disabling the reranking_model."""
        kb_name = basic_kb
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        if not openai_api_key:
            pytest.skip("OPENAI_API_KEY needed for this alter test.")

        alter_query_add = f"""
            ALTER KNOWLEDGE_BASE {kb_name}
            USING
                reranking_model = {{ 'provider': 'openai', 'model_name': 'gpt-4o', 'api_key': '{openai_api_key}' }};
        """
        self.query(alter_query_add)

        result_add = self.query(f"DESCRIBE KNOWLEDGE_BASE {kb_name};")
        assert result_add and '"provider": "openai"' in result_add[0]["RERANKING_MODEL"]

        alter_query_disable = f"""
            ALTER KNOWLEDGE_BASE {kb_name}
            USING
                reranking_model = false;
        """
        self.query(alter_query_disable)

        result_disable = self.query(f"DESCRIBE KNOWLEDGE_BASE {kb_name};")
        assert result_disable
        reranking_model_desc = result_disable[0].get("RERANKING_MODEL")
        assert reranking_model_desc is None or reranking_model_desc == "{}"

    @pytest.mark.xfail(reason="Bug: information_schema.knowledge_bases.PARAMS is not updated on ALTER")
    @pytest.mark.usefixtures("basic_kb")
    def test_alter_kb_preprocessing(self, basic_kb, use_binary):
        """Tests altering the preprocessing parameters by checking the PARAMS column."""
        kb_name = basic_kb

        alter_query = f"""
            ALTER KNOWLEDGE_BASE {kb_name}
            USING
                preprocessing = {{ 'chunk_size': 300, 'chunk_overlap': 50 }};
        """
        self.query(alter_query)

        time.sleep(1)

        result = self.query(f"SELECT PARAMS FROM information_schema.knowledge_bases WHERE name = '{kb_name}';")

        assert result, "Query to information_schema returned no results."

        params_json = result[0].get("PARAMS")
        assert params_json is not None, "PARAMS column is NULL."

        assert '"preprocessing":' in params_json, "The 'preprocessing' key was not added to the PARAMS column."
        assert '"chunk_size": 300' in params_json, "chunk_size was not set correctly in PARAMS."
        assert '"chunk_overlap": 50' in params_json, "chunk_overlap was not set correctly in PARAMS."


@pytest.fixture(scope="function")
def setup_trigger_db(request):
    """Function-scoped fixture to ensure a clean DB for each trigger test."""

    db_name = "trigger_test_db_local"

    source_table_name = "trigger_source_table"
    target_table_name = "trigger_target_table"

    params = {"user": "postgres", "password": "postgres", "host": "postgres", "port": 5432, "database": "postgres"}
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

    except (mysql.connector.Error, TimeoutError) as setup_err:
        pytest.skip(f"Trigger fixture setup failed. Ensure Docker environment is running. Error: {setup_err}")

    finally:
        print(f"\n--> [CLEANUP] Dropping tables and DATABASE: {db_name}")
        try:
            helper.query(f"DROP TABLE IF EXISTS {db_name}.{source_table_name};")
            helper.query(f"DROP TABLE IF EXISTS {db_name}.{target_table_name};")
        except mysql.connector.Error as e:
            print(f"Warning: Error dropping tables during cleanup: {e}")
            pass

        try:
            helper.query(f"DROP DATABASE IF EXISTS {db_name};")
        except mysql.connector.Error as e:
            print(f"Warning: Error dropping database during cleanup: {e}")
            pass


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLTriggers(BaseStuff):
    """Test suite for Trigger operations."""

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("setup_trigger_db")
    def test_trigger_lifecycle(self, setup_trigger_db, use_binary):
        db_name, source_table_name, target_table_name = setup_trigger_db
        trigger_name = "test_insert_trigger"
        test_id = 201  # Use a new ID that will be INSERTED
        inserted_message = "this message was inserted"
        try:
            # Ensure the target table is clean
            self.query(f"DELETE FROM {db_name}.{target_table_name};")

            # Pre-drop trigger to ensure clean state
            try:
                self.query(f"DROP TRIGGER {trigger_name};")
            except Exception:
                pass

            create_trigger_query = f"""
                CREATE TRIGGER {trigger_name}
                ON {db_name}.{source_table_name}
                (INSERT INTO {db_name}.{target_table_name} (id, message) SELECT id, message FROM TABLE_DELTA);
            """
            print("\n[DEBUG] Sending CREATE TRIGGER command...")
            self.query(create_trigger_query)

            wait_for_trigger_creation(self.query, trigger_name, timeout=20)

            print("[DEBUG] Schema check complete. Proceeding to functional firing test...")

            # Activate Trigger with an INSERT, not an UPDATE
            self.query(
                f"INSERT INTO {db_name}.{source_table_name} (id, message) VALUES ({test_id}, '{inserted_message}');"
            )

            # Poll the target table for the new result
            result = []
            max_wait_time = 60
            interval = 1
            max_interval = 8
            start_time = time.time()
            while time.time() - start_time < max_wait_time:
                result = self.query(f"SELECT id, message FROM {db_name}.{target_table_name} WHERE id = {test_id};")
                if result:
                    break
                time.sleep(interval)
                interval = min(interval * 2, max_interval)

            # Verify
            assert result, f"Trigger did not fire for id {test_id} within {max_wait_time}s."
            assert result[0]["message"] == inserted_message

        finally:
            try:
                self.query(f"DROP TRIGGER {trigger_name};")
            except Exception:
                pass


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
            try:
                self.query(f"DROP TRIGGER {trigger_name};")
            except Exception:
                pass

            self.query(create_query)
            wait_for_trigger_creation(self.query, trigger_name, timeout=20)

            with pytest.raises(Exception) as e:
                self.query(create_query)
            assert "already exists" in str(e.value).lower()
        finally:
            try:
                self.query(f"DROP TRIGGER {trigger_name};")
            except Exception:
                pass

    def test_create_trigger_on_non_existent_table(self, use_binary, request):
        trigger_name = "bad_trigger_local"
        create_query = f"CREATE TRIGGER {trigger_name} ON non_existent_db.non_existent_table (SELECT 1);"
        try:
            with pytest.raises(Exception) as e:
                self.query(create_query)
            error_str = str(e.value).lower()
            assert "no integration with name" in error_str or "unknown database" in error_str
        finally:
            try:
                self.query(f"DROP TRIGGER {trigger_name};")
            except Exception:
                pass

    def test_drop_non_existent_trigger(self, use_binary):
        trigger_name = "non_existent_trigger"
        try:
            self.query(f"DROP TRIGGER {trigger_name};")
        except Exception:
            pass

        with pytest.raises(Exception) as e:
            self.query(f"DROP TRIGGER {trigger_name};")
        error_str = str(e.value).lower()
        assert "doesn't exist" in error_str or "unknown trigger" in error_str


@pytest.mark.parametrize("use_binary", [False, True], indirect=True)
class TestMySQLQueryComposability(BaseStuff):
    """Test suite for advanced query composability (CTEs, Subqueries, UNIONs)."""

    db_name = "test_composability_db"

    @pytest.fixture(scope="class")
    def composability_db(self):
        """Class-scoped fixture to create the postgres DB."""
        print(f"\n--> [Fixture composability_db] Setting up database: {self.db_name}")
        db_details = {
            "type": "postgres",
            "connection_data": {
                "host": "samples.mindsdb.com",
                "port": "5432",
                "user": "demo_user",
                "password": "demo_password",
                "database": "demo",
                "schema": "demo",
            },
        }
        try:
            self.create_database(self.db_name, db_details)
            self.validate_database_creation(self.db_name)
            yield self.db_name
        finally:
            print(f"\n--> [Fixture composability_db] Tearing down database: {self.db_name}")
            self.query(f"DROP DATABASE IF EXISTS {self.db_name};")

    @pytest.fixture
    def use_binary(self, request):
        self.use_binary = request.param

    @pytest.mark.usefixtures("composability_db")
    def test_common_table_expression_with(self, use_binary):
        """
        Tests a query using a WITH clause (CTE).
        tests that you can define a temporary result set (cte) and then query that result set.
        """
        query = f"""
            WITH cte AS (
                SELECT * FROM {self.db_name}.home_rentals WHERE number_of_rooms = 2
            )
            SELECT * FROM cte LIMIT 5;
        """
        result = self.query(query)
        assert len(result) > 0
        assert all(row["number_of_rooms"] == 2 for row in result)

    @pytest.mark.usefixtures("composability_db")
    def test_union_operator(self, use_binary):
        """Tests a query using the UNION set operator.
        It tests that you can combine the results from two separate queries into one list.
        """
        query = f"""
            (SELECT sqft, location, number_of_rooms FROM {self.db_name}.home_rentals WHERE number_of_rooms = 1 LIMIT 5)
            UNION
            (SELECT sqft, location, number_of_rooms FROM {self.db_name}.home_rentals WHERE number_of_rooms = 2 LIMIT 5);
        """
        result = self.query(query)
        assert len(result) > 0
        assert all(row["number_of_rooms"] in (1, 2) for row in result)

    @pytest.mark.usefixtures("composability_db")
    def test_subquery_with_join_and_cte(self, use_binary):
        """
        Tests a subquery rewrite for the unsupported 'WHERE IN (SELECT...)' syntax.
        This tests CTE, UNION, and JOIN composability.
        """
        query = f"""
            WITH allowed_rooms AS (
                SELECT 1 as room_num
                UNION
                SELECT 3 as room_num
            )
            SELECT t1.*
            FROM {self.db_name}.home_rentals AS t1
            JOIN allowed_rooms AS t2 ON t1.number_of_rooms = t2.room_num
            LIMIT 10;
        """
        result = self.query(query)
        assert len(result) > 0
        assert all(row["number_of_rooms"] in (1, 3) for row in result)

    @pytest.mark.usefixtures("composability_db")
    def test_from_subquery(self, use_binary):
        """Tests a subquery in the FROM clause.
        It tests that you can run a query inside the FROM clause and use its results
        as the source table for an outer query.
        """
        query = f"""
            SELECT * FROM (
                SELECT * FROM {self.db_name}.home_rentals WHERE number_of_rooms = 2
            ) as sub_table
            LIMIT 5;
        """
        result = self.query(query)
        assert len(result) > 0
        assert all(row["number_of_rooms"] == 2 for row in result)
