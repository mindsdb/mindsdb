import os
import gc
import dill
import pandas as pd
from typing import Dict, List, Optional

import lightwood
from lightwood.api.types import JsonAI
from lightwood.mixer import LightGBM
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition, _module_from_code

from utils import unpack_jsonai_old_args, get_aliased_columns
from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.model.learn_process import brack_to_mod, rep_recur
from mindsdb.utilities.config import Config
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join, BinaryOperation, Identifier, Constant
from mindsdb_sql.parser.dialects.mindsdb import (
    RetrainPredictor,
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
        try:
            import lightwood
            year, major, minor, hotfix = lightwood.__version__.split('.')
            assert int(year) > 22 or (int(year) == 22 and int(major) >= 4)
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

    def native_query(self, query: str) -> Optional[object]:
        statement = self.parser(query, dialect=self.dialect)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            if model_name in self.get_tables():
                raise Exception("Error: this model already exists!")
            else:
                target = statement.targets[0].parts[-1]
                params = { 'target': target }
                if statement.order_by:
                    params['timeseries_settings'] = {
                        'is_timeseries': True,
                        'order_by': [str(col) for col in statement.order_by],
                        'group_by': [str(col) for col in statement.group_by],
                        'window': int(statement.window),
                        'horizon': int(statement.horizon),
                    }

                json_ai_override = statement.using if statement.using else {}

                unpack_jsonai_old_args(json_ai_override)

                # get training data from other integration
                handler = MDB_CURRENT_HANDLERS[str(statement.integration_name)]
                handler_query = self.parser(statement.query_str, dialect=self.handler_dialect)
                df = self._data_gather(handler, handler_query)

                json_ai_keys = list(lightwood.JsonAI.__dict__['__annotations__'].keys())
                json_ai = json_ai_from_problem(df, ProblemDefinition.from_dict(params)).to_dict()
                json_ai_override = brack_to_mod(json_ai_override)
                rep_recur(json_ai, json_ai_override)
                json_ai = JsonAI.from_dict(json_ai)

                code = code_from_json_ai(json_ai)
                predictor = predictor_from_code(code)
                predictor.learn(df)
                serialized_predictor = dill.dumps(predictor)

                all_models = self.storage.get('models')
                payload = {'jsonai': json_ai, 'predictor': serialized_predictor, 'code': code, 'stmt': statement}
                if all_models is not None:
                    all_models[model_name] = payload
                else:
                    all_models = {model_name: payload}
                self.storage.set('models', all_models)

        elif type(statement) == RetrainPredictor:
            model_name = statement.name.parts[-1]
            if model_name not in self.get_tables():
                raise Exception("Error: this model does not exist, so it can't be retrained. Train a model first.")

            all_models = self.storage.get('models')
            original_stmt = all_models[model_name]['stmt']

            handler = MDB_CURRENT_HANDLERS[str(original_stmt.integration_name)]
            handler_query = self.parser(original_stmt.query_str, dialect=self.handler_dialect)
            df = self._data_gather(handler, handler_query)
            predictor = self._load_predictor(all_models[model_name], model_name)
            predictor.adjust(df)
            all_models[model_name]['predictor'] = dill.dumps(predictor)
            self.storage.set('models', all_models)

        elif type(statement) == DropPredictor:
            to_drop = statement.name.parts[-1]
            all_models = self.storage.get('models')
            del all_models[to_drop]
            self.storage.set('models', all_models)

        else:
            raise Exception(f"Query type {type(statement)} not supported")

    def query(self, query) -> dict:
        model = self._get_model(query)
        values = self._recur_get_conditionals(query.where.args, {})
        df = pd.DataFrame.from_dict(values)
        df = self._call_predictor(df, model)
        return {'data_frame': df}

    def _recur_get_conditionals(self, args: list, values):
        """ Gets all the specified data from an arbitrary amount of AND clauses inside the WHERE statement """  # noqa
        if isinstance(args[0], Identifier) and isinstance(args[1], Constant):
            values[args[0].parts[0]] = [args[1].value]
        else:
            for op in args:
                values = {**values, **self._recur_get_conditionals([*op.args], {})}
        return values

    def _data_gather(self, handler, query):
        """
        Dispatcher to task-specialized data gathering methods.
        e.g. for time series tasks, gather data per partition and fuse into a single dataframe
        """  # noqa
        if query.order_by is not None:
            return self._ts_data_gather(handler, query)
        else:
            # trigger default gather
            return self._default_data_gather(handler, query)

    def _default_data_gather(self, handler, query):
        records = handler.query(query)['data_frame']
        df = pd.DataFrame.from_records(records)
        return df

    def _ts_data_gather(self, handler, query):
        # todo: this should all be replaced by the mindsdb_sql logic
        def _gather_partition(handler, query):
            # todo apply limit and date here (LATEST vs other cutoffs)
            # if 'date' == 'LATEST':
            #     pass
            # else:
            #     pass
            records = handler.query(query)['data_frame']
            return pd.DataFrame.from_records(records)  # [:10]  # todo remove forced cap, testing purposes only

        if True:  # todo  # query.group_by is None:
            df = _gather_partition(handler, query)
        else:
            groups = handler.query(query)['data_frame']
            dfs = []
            for group in groups:
                # query.where_stmt =  # todo turn BinaryOperation and other types into an AND with the respective group filter
                dfs.append(_gather_partition(handler, query))
            df = pd.concat(*dfs)

        return df

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

        data_handler_table = getattr(stmt.from_table, data_clause).parts[-1]
        data_handler_cols = list(set([t.parts[-1] for t in stmt.targets]))

        model = self._get_model(stmt)
        is_ts = model.problem_definition.timeseries_settings.is_timeseries

        if not is_ts:
            data_query = f"SELECT {','.join(data_handler_cols)} FROM {data_handler_table}"
            if stmt.where:
                data_query += f" WHERE {str(stmt.where)}"
            if stmt.limit:
                data_query += f" LIMIT {stmt.limit.value}"

            parsed_query = self.parser(data_query, dialect=self.dialect)
            model_input = pd.DataFrame.from_records(
                data_handler.query(parsed_query)['data_frame'])
        else:
            # for TS, fetch correct groups, build window context, predict and limit, using self._ts_data_gather
            # todo: call mindsdb_sql group by retrieval

            # 1) get all groups
            gby_col = model.problem_definition.timeseries_settings.group_by[0]  # todo add multiple group support
            oby_col = model.problem_definition.timeseries_settings.order_by[0]
            for col in [gby_col] + [oby_col]:
                if col not in data_handler_cols:
                    data_handler_cols.append(col)

            window = model.problem_definition.timeseries_settings.window
            groups_query = f"SELECT {gby_col} from {data_handler_table}"  # add DISTINCT once the parser supports it
            parsed_groups_query = self.parser(groups_query, dialect=self.handler_dialect)
            out = data_handler.query(parsed_groups_query)['data_frame']
            groups = list(pd.DataFrame.from_records(
                data_handler.query(parsed_groups_query)['data_frame']
            ).drop_duplicates().squeeze())

            # 2) get LATEST available date and window for each group
            latests = {}
            windows = {}

            for group in groups:
                # get latest observed timestamp
                latest_query = f"SELECT {oby_col} FROM {data_handler_table}"
                latest_query += f" WHERE {gby_col}='{group}'"
                parsed_latest_query = self.parser(latest_query, dialect=self.handler_dialect)

                # we order and limit the df instead of via SELECT because it doesn't support those operators (yet)
                df = pd.DataFrame.from_records(
                    data_handler.query(parsed_latest_query)['data_frame']
                )
                df = list(df.sort_values(oby_col, ascending=False).iloc[0].values)[0]
                latests[group] = df

                # get window context
                window_query = f"SELECT {','.join(data_handler_cols)} FROM {data_handler_table}"
                window_query += f" WHERE {gby_col}='{group}'"
                # as above, we order and limit the df instead of via SELECT because it doesn't support those operators
                parsed_window_query = self.parser(window_query, dialect=self.handler_dialect)
                df = pd.DataFrame.from_records(
                    data_handler.query(
                        parsed_window_query
                    )['data_frame']
                )

                if len(df) < window and not model.problem_definition.timeseries_settings.allow_incomplete_history:
                    raise Exception(f"Not enough data for group {group}. Either pass more historical context or train a predictor with the `allow_incomplete_history` argument set to True.")
                df = df.sort_values(oby_col, ascending=False).iloc[0:window]
                windows[group] = df

            # 3) concatenate all contexts into single data query
            model_input = pd.concat([v for k, v in windows.items()]).reset_index(drop=True)

        # get model output
        predictions = self._call_predictor(model_input, model)

        # rename columns
        model_input.columns = get_aliased_columns(list(model_input.columns), model_alias, stmt.targets, mode='input')
        predictions.columns = get_aliased_columns(list(predictions.columns), model_alias, stmt.targets, mode='output')

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
        if 'original_index' in predictions.columns:
            predictions = predictions.sort_values(by='original_index')
        return df.join(predictions)


if __name__ == '__main__':
    # TODO: turn this into tests

    data_handler_name = 'mysql_handler'
    # todo: MDB needs to expose all available handlers through some sort of global state DB
    # todo: change data gathering logic if task is TS, use self._data_gather or similar
    # todo: eventually we would maybe do `from mindsdb.handlers import MDB_CURRENT_HANDLERS` registry
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

    model_name = 'lw_test_predictor'
    # try:
    #     print('dropping predictor...')
    #     cls.native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     print('failed to drop')
    #     pass

    print(cls.get_tables())

    data_table_name = 'home_rentals_subset'
    target = 'rental_price'
    if model_name not in cls.get_tables():
        query = f"CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target}"
        cls.native_query(query)

    print(cls.describe_table(f'{model_name}'))

    # try single WHERE condition
    query = f"SELECT target from {model_name} WHERE sqft=100"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.query(parsed)['data_frame']

    # try multiple
    query = f"SELECT target from {model_name} WHERE sqft=100 AND number_of_rooms=2 AND number_of_bathrooms=1"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.query(parsed)['data_frame']

    into_table = 'test_join_into_lw'
    query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.sqft from {data_handler_name}.{data_table_name} AS ta JOIN {model_name} AS tb LIMIT 10"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.join(parsed, data_handler, into=into_table)

    # checks whether `into` kwarg does insert into the table or not
    q = f"SELECT * FROM {into_table}"
    qp = cls.parser(q, dialect='mysql')
    assert len(data_handler.query(qp)['data_frame']) > 0

    # retrain syntax
    query = f"RETRAIN {model_name}"
    cls.native_query(query)

    # try:
    #     data_handler.native_query(f"DROP TABLE test.{into_table}")
    # except:
    #     pass

    # try:
    #     cls.native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    # Test 2: add custom JsonAi
    model_name = 'lw_test_predictor2'
    # try:
    #     cls.native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    if model_name not in cls.get_tables():
        using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
        query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} USING {using_str}'
        cls.native_query(query)

    m = cls._load_predictor(cls.storage.get('models')[model_name], model_name)
    assert len(m.ensemble.mixers) == 1
    assert isinstance(m.ensemble.mixers[0], LightGBM)

    # Timeseries predictor
    model_name = 'lw_test_predictor3'
    target = 'Traffic'
    data_table_name = 'arrival'
    oby = 'T'
    gby = 'Country'
    window = 8
    horizon = 4

    model_name = 'lw_test_predictor3'
    # try:
    #     cls.native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    if model_name not in cls.get_tables():
        query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} ORDER BY {oby} GROUP BY {gby} WINDOW {window} HORIZON {horizon}'
        cls.native_query(query)

    p = cls.storage.get('models')
    m = cls._load_predictor(p[model_name], model_name)
    assert m.problem_definition.timeseries_settings.is_timeseries

    # get predictions from a time series model
    # todo: limit in this case should be checked/compared against model's own HORIZON
    into_table = 'test_join_tsmodel_into_lw'
    query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from {data_handler_name}.{data_table_name} AS ta JOIN {model_name} AS tb WHERE ta.{oby} > LATEST LIMIT 10"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.join(parsed, data_handler, into=into_table)
