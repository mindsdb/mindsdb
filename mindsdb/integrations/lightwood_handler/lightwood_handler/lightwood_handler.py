import os
import gc
import dill
import pandas as pd
from typing import Dict, List, Optional

import lightwood
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition, _module_from_code

from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.utilities.config import Config
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join
from mindsdb_sql.parser.dialects.mindsdb import (
    # RetrainPredictor,  # todo
    CreatePredictor,
    DropPredictor
)


class LightwoodHandler(PredictiveHandler):
    def __init__(self, name):
        """ Lightwood AutoML integration """  # noqa
        super().__init__(name)
        self.storage = None
        self.parser = parse_sql
        self.dialect = 'mindsdb'
        self.handler_dialect = 'mysql'

    def connect(self, **kwargs) -> Dict[str, int]:
        """ Setup storage handler and check lightwood version """  # noqa
        self.storage = SqliteStorageHandler(context=self.name, config=kwargs['config'])
        return self.check_status()

    def check_status(self) -> Dict[str, int]:
        """ Checks that the connection is, as expected, an MlflowClient instance. """  # noqa
        # todo: potentially nothing to do here, as we can assume user to install requirements first
        try:
            import lightwood
            year, major, minor, hotfix = lightwood.__version__.split('.')
            assert int(year) >= 22
            assert int(major) >= 2
            assert int(minor) >= 3
            print("Lightwood OK!")
            return {'status': '200'}
        except AssertionError as e:
            print("Cannot import lightwood!")
            return {'status': '503', 'error': e}

    def get_tables(self) -> List:
        """ Returns list of model names (that have been succesfully linked with CREATE PREDICTOR) """  # noqa
        models = self.storage.get('models')
        return list(models.keys()) if models else []

    def describe_table(self, table_name: str) -> Dict:
        """ For getting standard info about a table. e.g. data types """  # noqa
        if table_name not in self.get_tables():
            print("Table not found.")
            return {}
        return self.storage.get('models')[model_name]['jsonai']

    def run_native_query(self, query_str: str) -> Optional[object]:
        statement = self.parser(query_str, dialect=self.dialect)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            if model_name in self.get_tables():
                print("Error: this model already exists!")
            else:
                target = statement.targets[0].parts[-1]
                params = { 'target': target }
                json_ai_override = {}
                json_ai_keys = list(lightwood.JsonAI.__dict__['__annotations__'].keys())
                # todo find a way to unnest dot notation and do override in correct way
                for k in statement.using:
                    if '.' in k:
                        k = k.split('.')[0]
                    if k in json_ai_keys:
                        json_ai_override[k] = statement.using[k]

                # get training data from other integration
                # todo: MDB needs to expose all available handlers through some sort of global state
                # todo: change data gathering logic if task is TS, use self._data_gather or similar
                handler = MDB_CURRENT_HANDLERS[str(statement.integration_name)]
                handler_query = self.parser(statement.query_str, dialect=self.handler_dialect)
                records = handler.select_query(targets=handler_query.targets, from_stmt=handler_query.from_table, where_stmt=handler_query.where)
                df = pd.DataFrame.from_records(records)[:10]  # todo remove forced cap

                jsonai = json_ai_from_problem(df, ProblemDefinition.from_dict(params))
                for key in json_ai_override:
                    setattr(jsonai, key, json_ai_override[key])

                code = code_from_json_ai(jsonai)
                predictor = predictor_from_code(code)
                predictor.learn(df)
                serialized_predictor = dill.dumps(predictor)

                all_models = self.storage.get('models')
                payload = {'jsonai': jsonai, 'predictor': serialized_predictor, 'code': code}
                if all_models is not None:
                    all_models[model_name] = payload
                else:
                    all_models = {model_name: payload}
                self.storage.set('models', all_models)

        elif type(statement) == DropPredictor:
            to_drop = statement.name.parts[-1]
            all_models = self.storage.get('models')
            del all_models[to_drop]
            self.storage.set('models', all_models)

        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def select_query(self, stmt) -> pd.DataFrame:
        # todo: expand to arbitrary amount of values
        model = self._get_model(stmt)

        df = pd.DataFrame.from_dict({stmt.where.args[0].parts[0]: [stmt.where.args[1].value]})
        return self._call_predictor(df, model)

    def _data_gather(self):
        # todo: this would be the pipeline for specialized tasks
        # e.g. check whether it's a TS task, then we gather data with the relevant query plan
        # e.g. if it's an image task, we go and fetch all images
        pass

    def join(self, stmt, data_handler: BaseHandler, into: Optional[str] = None) -> pd.DataFrame:
        """
        Batch prediction using the output of a query passed to a data handler as input for the model.
        """  # noqa
        # tag data and predictive handlers
        if len(stmt.from_table.left.parts) == 1:
            model_clause = 'left'
            data_clause = 'right'
        else:
            model_clause = 'right'
            data_clause = 'left'
        model_alias = str(getattr(stmt.from_table, model_clause).alias)

        data_handler_table = getattr(stmt.from_table, data_clause).parts[-1]  # todo should be ".".join(...) if data handlers support more than one table
        data_handler_cols = list(set([t.parts[-1] for t in stmt.targets]))

        data_query = f"""SELECT {','.join(data_handler_cols)} FROM {data_handler_table}"""
        if stmt.where:
            data_query += f" WHERE {str(stmt.where)}"
        if stmt.limit:
            # todo integration should handle this depending on type of query... e.g. if it is TS, then we have to fetch correct groups first and limit later, use self._data_gather or similar
            data_query += f" LIMIT {stmt.limit.value}"

        parsed_query = self.parser(data_query, dialect=self.dialect)
        model_input = pd.DataFrame.from_records(
            data_handler.select_query(
                parsed_query.targets,
                parsed_query.from_table,
                parsed_query.where
            ))

        # rename columns
        aliased_columns = list(model_input.columns)
        for col in stmt.targets:
            if str(col.parts[0]) != model_alias and col.alias is not None:
                # assumes mdb_sql will alert if there are two columns with the same alias
                aliased_columns[aliased_columns.index(col.parts[-1])] = str(col.alias)
        model_input.columns = aliased_columns

        # get model output
        model = self._get_model(stmt)
        predictions = self._call_predictor(model_input, model)

        # rename columns
        aliased_columns = list(predictions.columns)
        for col in stmt.targets:
            if col.parts[0] == model_alias and col.alias is not None:
                aliased_columns[aliased_columns.index('prediction')] = str(col.alias)
        predictions.columns = aliased_columns

        if into:
            try:
                data_handler.select_into(into, predictions)
            except Exception as e:
                print("Error when trying to store the JOIN output in data handler.")

        return predictions

    def _get_model(self, stmt):
        if type(stmt.from_table) == Join:
            model_name = stmt.from_table.right.parts[-1]
        else:
            model_name = stmt.from_table.parts[-1]

        if not model_name in self.get_tables():
            raise Exception("Error, not found. Please create this predictor first.")

        predictor_dict = self._get_model_info(model_name)
        predictor = self._load_predictor(predictor_dict, model_name)
        return predictor

    def _get_model_info(self, model_name):
        """ Returns a dictionary with three keys: 'jsonai', 'predictor' (serialized), and 'code'. """  # noqa
        return self.storage.get('models')[model_name]

    def _load_predictor(self, predictor_dict, name):
        # todo update lightwood method (predictor_from_state) so that it can take a memory representation of the predictor object OR a file path, and call that instead
        try:
            module_name = None
            return dill.loads(predictor_dict['predictor'])
        except Exception as e:
            module_name = str(e).lstrip("No module named '").split("'")[0]

            try:
                del sys.modules[module_name]
            except Exception:
                pass

            gc.collect()
            _module_from_code(predictor_dict['code'], module_name)
            return dill.loads(predictor_dict['predictor'])

    def _call_predictor(self, df, predictor):
        predictions = predictor.predict(df)
        return df.join(predictions)


if __name__ == '__main__':
    # TODO: turn this into tests

    data_handler_name = 'mysql_handler'
    MDB_CURRENT_HANDLERS = {
        data_handler_name: MySQLHandler('test_handler', **{
            "host": "localhost",
            "port": "3306",
            "user": "root",
            "password": "root",
            "database": "test",
            "ssl": False
        })
    }  # todo: handler CRUD should be done at mindsdb top-level
    data_handler = MDB_CURRENT_HANDLERS[data_handler_name]
    print(data_handler.check_status())

    cls = LightwoodHandler('LWtest')
    config = Config()
    print(cls.connect(config={'path': config['paths']['root'], 'name': 'lightwood_handler.db'}))

    # try:
    #     print('dropping predictor...')
    #     cls.run_native_query(f"DROP PREDICTOR {registered_model_name}")
    # except:
    #     print('failed to drop')
    #     pass

    print(cls.get_tables())

    data_table_name = 'home_rentals_subset'
    model_name = 'lw_test_predictor'
    target = 'rental_price'
    if model_name not in cls.get_tables():
        query = f"CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target}"
        cls.run_native_query(query)

    print(cls.describe_table(f'{model_name}'))

    query = f"SELECT target from {model_name} WHERE sqft=100"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.select_query(parsed)

    into_table = 'test_join_into_lw'
    query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.sqft from {data_handler_name}.{data_table_name} AS ta JOIN {model_name} AS tb LIMIT 10"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.join(parsed, data_handler, into=into_table)

    # checks whether `into` kwarg does insert into the table or not
    q = f"SELECT * FROM {into_table}"
    qp = cls.parser(q, dialect='mysql')
    assert len(data_handler.select_query(qp.targets, qp.from_table, qp.where)) > 0

    try:
        data_handler.run_native_query(f"DROP TABLE test.{into_table}")
    except:
        pass

    # try:
    #     cls.run_native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    # Test 2: add custom JsonAi
    model_name = 'lw_test_predictor2'
    try:
        cls.run_native_query(f"DROP PREDICTOR {model_name}")
    except:
        pass


    if model_name not in cls.get_tables():
        # 'CREATE PREDICTOR lw_test_predictor2 FROM mysql_handler (SELECT * FROM test.home_rentals_subset) PREDICT rental_price USING model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
        using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
        query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} USING {using_str}'
        cls.run_native_query(query)

    p = cls.storage.get('models')
