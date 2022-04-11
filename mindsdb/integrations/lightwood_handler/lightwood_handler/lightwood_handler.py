import os
import gc
import dill
import pandas as pd
from typing import Dict, List, Optional

import lightwood
from lightwood.api.types import JsonAI
from lightwood.api.high_level import json_ai_from_problem, predictor_from_code, code_from_json_ai, ProblemDefinition, _module_from_code

from mindsdb.integrations.libs.base_handler import BaseHandler, PredictiveHandler
from mindsdb.integrations.libs.storage_handler import SqliteStorageHandler
from mindsdb.integrations.mysql_handler.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.interfaces.model.learn_process import brack_to_mod, rep_recur
from mindsdb.utilities.config import Config
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Join, BinaryOperation, Identifier, Constant
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
        try:
            import lightwood  # todo maybe we shouldn't even check, as we can assume user has installed requisites for the handler
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

    def run_native_query(self, query_str: str) -> Optional[object]:
        statement = self.parser(query_str, dialect=self.dialect)

        if type(statement) == CreatePredictor:
            model_name = statement.name.parts[-1]

            if model_name in self.get_tables():
                print("Error: this model already exists!")
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

                # this is copied and adapted from ModelController._unpack_old_args, should be a helper function
                while '.' in str(list(json_ai_override.keys())):
                    for k in list(json_ai_override.keys()):
                        if '.' in k:
                            nks = k.split('.')
                            obj = json_ai_override
                            for nk in nks[:-1]:
                                if nk not in obj:
                                    obj[nk] = {}
                                obj = obj[nk]
                            obj[nks[-1]] = json_ai_override[k]
                            del json_ai_override[k]

                # get training data from other integration
                # todo: MDB needs to expose all available handlers through some sort of global state
                # todo: change data gathering logic if task is TS, use self._data_gather or similar
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
                payload = {'jsonai': json_ai, 'predictor': serialized_predictor, 'code': code}
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
        model = self._get_model(stmt)
        # if 'LATEST' in str(stmt.where):
        #     stmt = self._get_latest_oby(stmt)  # todo: it would be easy if I had access to the handler here, just query the handler to get the latest available date then proceed as usual
        # todo: with signatures as they stand, the way to do it is to actually fetch latest from model internal data, and emit forecast for that
        # TODO: check with max whether there is support for latest without joining. if so, this is a problem. if not, then it's actually fine.
        # todo: for now, will just ignore this possibility
        values = self._recur_get_conditionals(stmt.where.args, {})
        df = pd.DataFrame.from_dict(values)
        return self._call_predictor(df, model)

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
        records = handler.select_query(targets=query.targets, from_stmt=query.from_table, where_stmt=query.where)
        df = pd.DataFrame.from_records(records)  #[:10]  # todo remove forced cap, testing purposes only
        return df

    def _ts_data_gather(self, handler, query):
        def _gather_partition(handler, query):
            # todo apply limit and date here (LATEST vs other cutoffs)
            # if 'date' == 'LATEST':
            #     pass
            # else:
            #     pass
            records = handler.select_query(targets=query.targets, from_stmt=query.from_table, where_stmt=query.where)
            return pd.DataFrame.from_records(records)  # [:10]  # todo remove forced cap, testing purposes only

        if True:  # todo  # query.group_by is None:
            df = _gather_partition(handler, query)
        else:
            groups = handler.select_query(targets=query.group_by, from_stmt=query.from_table, where_stmt=query.where)
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

        data_handler_table = getattr(stmt.from_table, data_clause).parts[-1]  # todo should be ".".join(...) if data handlers support more than one table?
        data_handler_cols = list(set([t.parts[-1] for t in stmt.targets]))

        model = self._get_model(stmt)
        is_ts = model.problem_definition.timeseries_settings.is_timeseries

        if not is_ts:
            data_query = f"""SELECT {','.join(data_handler_cols)} FROM {data_handler_table}"""
            if stmt.where:
                data_query += f" WHERE {str(stmt.where)}"
            if stmt.limit:
                # todo integration should handle this depending on type of query... e.g. if it is TS, then we have to fetch correct groups first and limit later, use self._data_gather or similar
                data_query += f" LIMIT {stmt.limit.value}"
        else:
            # 1. get all groups
            # 2. get LATEST available date for each group
            # 2. get WINDOW context (if not enough, check whether tss.allow_incomplete_history and depending on that either pass incomplete or not)
            # 3. concatenate all contexts into single data query

            # todo: how to retrieve groups? we can limit ourselves to the ones discovered at training (stored inside the predictor metadata),
            # todo: however it is also possible for new groups to exist and for those we should probably still create predictions, right?
            # todo: this means doing a query in the data handler to get the available groups in the table after doing the right order by filter (e.g. LATEST or any other particular cutoff)
            # for group in data_handler_group_query_after_date_cutoff
            pass

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

    # try single WHERE condition
    query = f"SELECT target from {model_name} WHERE sqft=100"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.select_query(parsed)

    # try multiple
    query = f"SELECT target from {model_name} WHERE sqft=100 AND number_of_rooms=2 AND number_of_bathrooms=1"
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
    # try:
    #     cls.run_native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    if model_name not in cls.get_tables():
        using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": True}}]}'
        query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} USING {using_str}'
        cls.run_native_query(query)
        # todo write assertion to check new predictor only uses LightGBM model (it does happen, but assert is missing)

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
    #     cls.run_native_query(f"DROP PREDICTOR {model_name}")
    # except:
    #     pass

    if model_name not in cls.get_tables():
        query = f'CREATE PREDICTOR {model_name} FROM {data_handler_name} (SELECT * FROM test.{data_table_name}) PREDICT {target} ORDER BY {oby} GROUP BY {gby} WINDOW {window} HORIZON {horizon}'
        cls.run_native_query(query)

        # todo missing assert that predictor is a TS one
        p = cls.storage.get('models')
        _ = cls._load_predictor(p[model_name], model_name)

    # get predictions from a time series model
    # TODO: for now, I will just ignore this use case, I don't think we even support it as of now, so let's just consider a normal JOIN
    # query = f"SELECT target from {model_name} WHERE {oby} > LATEST"
    # parsed = cls.parser(query, dialect=cls.dialect)
    # predicted = cls.select_query(parsed)

    # todo: limit in this case should be checked/compared against model's own HORIZON
    into_table = 'test_join_tsmodel_into_lw'
    query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from {data_handler_name}.{data_table_name} AS ta JOIN {model_name} AS tb WHERE ta.{oby} > LATEST LIMIT 10"
    parsed = cls.parser(query, dialect=cls.dialect)
    predicted = cls.join(parsed, data_handler, into=into_table)

