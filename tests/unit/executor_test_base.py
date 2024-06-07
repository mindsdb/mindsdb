import copy
import datetime as dt
import json
import os
import sys
import tempfile
import time
from unittest import mock
from pathlib import Path
from prometheus_client import REGISTRY

import duckdb
import numpy as np
import pandas as pd
from mindsdb.utilities import log
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql import parse_sql

logger = log.getLogger(__name__)


def unload_module(path):
    # remove all modules started with path
    import sys

    to_remove = []
    for module_name in sys.modules:
        if module_name.startswith(path + ".") or module_name == path:
            to_remove.append(module_name)
    to_remove.sort(reverse=True)
    for module_name in to_remove:
        sys.modules.pop(module_name)


class BaseUnitTest:
    """
    mindsdb instance with temporal database and config
    """

    @staticmethod
    def setup_class(cls):
        # remove imports of mindsdb in previous tests
        unload_module("mindsdb")

        # database temp file
        cls.db_file = tempfile.mkstemp(prefix="mindsdb_db_")[1]

        # config
        config = {"storage_db": "sqlite:///" + cls.db_file}
        # config temp file
        fdi, cfg_file = tempfile.mkstemp(prefix="mindsdb_conf_")

        with os.fdopen(fdi, "w") as fd:
            json.dump(config, fd)

        os.environ["MINDSDB_CONFIG_PATH"] = cfg_file

        # initialize config
        from mindsdb.utilities.config import Config

        Config()

        from mindsdb.interfaces.storage import db

        db.init()
        cls.db = db

        from multiprocessing import dummy

        # We might not have torch installed. So ignore any errors
        try:
            mp_patcher = mock.patch("torch.multiprocessing.get_context").__enter__()
            mp_patcher.side_effect = lambda x: dummy
        except Exception:
            mp_patcher = mock.patch("multiprocessing.get_context").__enter__()
            mp_patcher.side_effect = lambda x: dummy

    @staticmethod
    def teardown_class(cls):
        # remove tmp db file
        cls.db.session.close()
        try:
            os.unlink(cls.db_file)
        except PermissionError as e:
            logger.warning('Unable to clean up temporary database file: %s', str(e))

        # remove environ for next tests
        del os.environ["MINDSDB_DB_CON"]

        # remove import of mindsdb for next tests
        unload_module("mindsdb")

    def setup_method(self):
        self._dummy_db_path = os.path.join(tempfile.mkdtemp(), '_mindsdb_duck_db')
        self.clear_db(self.db)
        self.reset_prom_collectors()

    def teardown_method(self):
        try:
            os.unlink(self._dummy_db_path)
        except (PermissionError, FileNotFoundError) as e:
            logger.warning('Unable to clean up temporary database file: %s', str(e))

    def clear_db(self, db):
        # drop
        db.session.rollback()
        db.Base.metadata.drop_all(db.engine)

        # create
        db.Base.metadata.create_all(db.engine)

        # fill with data
        r = db.Integration(name="files", data={}, engine="files")
        db.session.add(r)
        r = db.Integration(name="views", data={}, engine="views")
        db.session.add(r)
        r = db.Integration(name="dummy_data", data={'db_path': self._dummy_db_path}, engine="dummy_data")
        db.session.add(r)

        # Lightwood should always be last (else tests break, why?)
        r = db.Integration(name="lightwood", data={}, engine="lightwood")
        db.session.add(r)

        db.session.flush()

        self.lw_integration_id = r.id

        # default project
        r = db.Project(name="mindsdb")
        db.session.add(r)

        db.session.commit()
        return db

    def set_data(self, table, data):
        con = duckdb.connect(self._dummy_db_path)
        con.execute('DROP TABLE IF EXISTS {}'.format(table))
        con.execute('CREATE TABLE {} AS SELECT * FROM data'.format(table))

    def wait_predictor(self, project, name, timeout=100):
        """
        Wait for the predictor to be created,
        raising an exception if predictor creation fails or exceeds timeout
        """
        for attempt in range(timeout):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                status = ret["STATUS"][0]
                if status == "complete":
                    return
                elif status == "error":
                    raise RuntimeError("Predictor failed", ret["ERROR"][0])
            time.sleep(0.5)
        raise RuntimeError("Predictor wasn't created")

    def run_sql(self, sql):
        """Execute SQL and return a DataFrame, raising an AssertionError if an error occurs"""
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None, f"SQL execution failed with error: {ret.error_code}"
        if ret.data is not None:
            return ret.data.to_df()

    @staticmethod
    def ret_to_df(ret):
        # converts executor response to dataframe
        return ret.data.to_df()

    def reset_prom_collectors(self) -> None:
        """Resets collectors in the default Prometheus registry.

        Modifies the `REGISTRY` registry. Supposed to be called at the beginning
        of individual test functions. Else registry is reused across test functions
        and so we can run into errors like duplicate metrics or unexpected values
        for metrics.
        """
        # Unregister all collectors.
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            REGISTRY.unregister(collector)


class BaseExecutorTest(BaseUnitTest):
    """
    Set up executor: mock data handler
    """

    def setup_method(self):
        super().setup_method()
        self.set_executor()

    def set_executor(
        self,
        mock_lightwood=False,
        mock_model_controller=False,
        import_dummy_ml=False,
        import_dummy_llm=False,
    ):
        # creates executor instance with mocked model_interface
        from mindsdb.api.executor.controllers.session_controller import (
            SessionController,
        )
        from mindsdb.api.executor.command_executor import (
            ExecuteCommands,
        )
        from mindsdb.interfaces.database.integrations import integration_controller
        from mindsdb.interfaces.file.file_controller import FileController
        from mindsdb.interfaces.model.model_controller import ModelController
        from mindsdb.utilities.context import context as ctx

        self.file_controller = FileController()

        if mock_model_controller:
            model_controller = mock.Mock()
            self.mock_model_controller = model_controller
        else:
            model_controller = ModelController()

        # no predictors yet
        # self.mock_model_controller.get_models.side_effect = lambda: []

        if import_dummy_ml:
            test_handler_path = os.path.dirname(__file__)
            sys.path.append(test_handler_path)

            handler_dir = Path(test_handler_path) / 'dummy_ml_handler'
            integration_controller.import_handler('', handler_dir)

            if not integration_controller.handlers_import_status['dummy_ml']['import']['success']:
                error = integration_controller.handlers_import_status['dummy_ml']['import']['error_message']
                raise Exception(f"Can not import: {str(handler_dir)}: {error}")

        if import_dummy_llm:
            test_handler_path = os.path.dirname(__file__)
            sys.path.append(test_handler_path)

            handler_dir = Path(test_handler_path) / 'dummy_llm_handler'
            integration_controller.import_handler('', handler_dir)

            if not integration_controller.handlers_import_status['dummy_llm']['import']['success']:
                error = integration_controller.handlers_import_status['dummy_llm']['import']['error_message']
                raise Exception(f"Can not import: {str(handler_dir)}: {error}")

        if mock_lightwood:
            predict_patcher = mock.patch("mindsdb.integrations.libs.ml_exec_base.BaseMLEngineExec.predict")
            self.mock_predict = predict_patcher.__enter__()

            create_patcher = mock.patch("mindsdb.integrations.handlers.lightwood_handler.Handler.create")
            self.mock_create = create_patcher.__enter__()

        ctx.set_default()
        sql_session = SessionController()
        sql_session.database = "mindsdb"
        sql_session.integration_controller = integration_controller

        self.command_executor = ExecuteCommands(sql_session)

        # disable cache. it is need to check predictor input
        config_patch = mock.patch("mindsdb.utilities.cache.FileCache.get")
        self.mock_config = config_patch.__enter__()
        self.mock_config.side_effect = lambda x: None

    def teardown_method(self):
        # Don't want cache to pick up a stale version with the wrong duckdb_path.
        self.command_executor.session.integration_controller.delete('dummy_data')

    def save_file(self, name, df):
        file_path = tempfile.mktemp(prefix="mindsdb_file_")
        df.to_parquet(file_path)
        self.file_controller.save_file(name, file_path, name)

    def set_handler(self, mock_handler, name, tables, engine="postgres"):
        # integration
        # delete by name
        r = self.db.Integration.query.filter_by(name=name).first()
        if r is not None:
            self.db.session.delete(r)

        # create
        r = self.db.Integration(
            name=name,
            data={'password': 'secret'},
            engine=engine
        )
        self.db.session.add(r)
        self.db.session.commit()

        from mindsdb.integrations.libs.response import RESPONSE_TYPE
        from mindsdb.integrations.libs.response import HandlerResponse as Response

        def handler_response(df):
            response = Response(RESPONSE_TYPE.TABLE, df)
            return response

        def get_tables_f():
            tables_ar = []
            for table in tables:
                tables_ar.append(
                    {
                        "table_schema": "public",
                        "table_name": table,
                        "table_type": "BASE TABLE",
                    }
                )

            return handler_response(
                pd.DataFrame(
                    [
                        {
                            "table_schema": "public",
                            "table_name": "table1",
                            "table_type": "BASE TABLE",
                        }
                    ]
                )
            )

        mock_handler().get_tables.side_effect = get_tables_f

        def get_columns_f(table_name):
            type = "varchar"
            cols = []
            for col, typ in tables[table_name].dtypes.items():
                if pd.api.types.is_integer_dtype(typ):
                    type = "integer"
                elif pd.api.types.is_float_dtype(typ):
                    type = "float"
                elif pd.api.types.is_datetime64_dtype(typ):
                    type = "datetime"
                cols.append({"Field": col, "Type": type})
            return handler_response(pd.DataFrame(cols))

        mock_handler().get_columns.side_effect = get_columns_f

        # use duckdb to execute query for integrations
        def native_query_f(query):
            con = duckdb.connect(database=":memory:")

            for table, df in tables.items():
                con.register(table, df)
            try:
                con.execute(query)
                columns = [c[0] for c in con.description]
                result_df = pd.DataFrame(con.fetchall(), columns=columns)

                result_df = result_df.replace({np.nan: None})
            except Exception:
                # it can be not supported command like update or insert
                result_df = pd.DataFrame()
            for table in tables.keys():
                con.unregister(table)

            con.close()
            return handler_response(result_df)

        def query_f(query):
            renderer = SqlalchemyRender("postgres")
            query_str = renderer.get_string(query, with_failback=True)
            return native_query_f(query_str)

        mock_handler().native_query.side_effect = native_query_f

        mock_handler().query.side_effect = query_f

    def set_project(self, project):
        r = self.db.Project.query.filter_by(name=project["name"]).first()
        if r is not None:
            self.db.session.delete(r)

        r = self.db.Project(
            id=1,
            name=project["name"],
        )
        self.db.session.add(r)
        self.db.session.commit()


class BaseExecutorDummyML(BaseExecutorTest):
    """
    Set up executor: mock data handler
    """

    def setup_method(self):
        super().setup_method()
        self.set_executor(import_dummy_ml=True)

    def run_sql(self, sql, throw_error=True, database='mindsdb'):
        self.command_executor.session.database = database
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        if throw_error:
            assert ret.error_code is None
        if ret.data is not None:
            return ret.data.to_df()


class BaseExecutorDummyLLM(BaseExecutorTest):
    """
    Set up executor: mock LLM handler
    """

    def setup_method(self):
        super().setup_method()
        self.set_executor(import_dummy_llm=True)


class BaseExecutorMockPredictor(BaseExecutorTest):
    """
    Set up executor: mock data handler and LW handler
    """

    def setup_method(self):
        super().setup_method()
        self.set_executor(mock_lightwood=True, mock_model_controller=True)

    def set_predictor(self, predictor):
        # fill model_interface mock with predictor data for test case

        # clear calls
        self.mock_model_controller.reset_mock()
        self.mock_predict.reset_mock()
        self.mock_create.reset_mock()

        # remove previous predictor record
        r = self.db.Predictor.query.filter_by(name=predictor["name"]).first()
        if r is not None:
            self.db.session.delete(r)

        if "problem_definition" not in predictor:
            predictor["problem_definition"] = {"timeseries_settings": {"is_timeseries": False}}

        # add predictor to table
        r = self.db.Predictor(
            name=predictor["name"],
            data={"dtypes": predictor["dtypes"]},
            learn_args=predictor["problem_definition"],
            to_predict=predictor["predict"],
            integration_id=self.lw_integration_id,
            project_id=1,
            status="complete",
        )
        self.db.session.add(r)
        self.db.session.commit()

        def predict_f(_model_name, df, pred_format="dict", *args, **kargs):
            explain_arr = []
            data = df.to_dict('records')

            predicted_value = predictor["predicted_value"]
            target = predictor["predict"]

            meta = {
                # 'select_data_query': None, 'when_data': None,
                "original": None,
                "confidence": 0.8,
                "anomaly": None,
            }

            data = copy.deepcopy(data)
            for row in data:
                # row = row.copy()
                exp_row = {
                    "predicted_value": predictor["predicted_value"],
                    "confidence": 0.9999,
                    "anomaly": None,
                    "truth": None,
                }
                explain_arr.append({predictor["predict"]: exp_row})

                row[target] = predicted_value
                # dict_arr.append({predictor['predict']: row})

                for k, v in meta.items():
                    row[f"{target}_{k}"] = v
                row[f"{target}_explain"] = str(exp_row)

            if pred_format == "explain":
                return explain_arr
            return pd.DataFrame(data)

        predictor_record = {
            "version": None,
            "is_active": None,
            "status": "complete",
            "current_phase": None,
            "accuracy": 0.9992752583404642,
            "data_source": None,
            "update": "available",
            "data_source_name": None,
            "mindsdb_version": "22.3.5.0",
            "error": None,
            "train_end_at": None,
            "updated_at": dt.datetime(2022, 5, 12, 16, 40, 26),
            "created_at": dt.datetime(2022, 4, 4, 14, 48, 39),
        }
        predictor["dtype_dict"] = predictor["dtypes"]
        predictor_record.update(predictor)

        def get_model_data_f(name, *args):
            if name != predictor["name"]:
                raise Exception(f"Model does not exists: {name}")
            return predictor_record

        # inject predictor info to model interface
        self.mock_predict.side_effect = predict_f
        self.mock_model_controller.get_models.side_effect = lambda: [predictor_record]
        self.mock_model_controller.get_model_data.side_effect = get_model_data_f

    def execute(self, sql):
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        if ret.error_code is not None:
            raise Exception()
        if ret.data is not None:
            ret.records = ret.data.records
        return ret
