import pandas as pd
from mindsdb_sql_parser.ast import Constant, Identifier, Insert, OrderBy, TableColumn
from mindsdb_sql_parser.ast.mindsdb import (
    CreateJob,
    CreateMLEngine,
    CreatePredictor,
    FinetunePredictor,
    RetrainPredictor,
)

import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.classes import Responder
from mindsdb.api.mongo.classes.query_sql import run_sql_command
from mindsdb.api.mongo.responders.aggregate import aggregate_to_ast
from mindsdb.api.mongo.responders.find import find_to_ast
from mindsdb.api.mongo.utilities.mongodb_parser import MongodbParser
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.utilities import log
from mindsdb.utilities.config import config

logger = log.getLogger(__name__)
default_project = config.get("default_project")


class Responce(Responder):
    when = {"insert": helpers.is_true}

    def result(self, query, request_env, mindsdb_env, session):
        try:
            res = self._result(query, request_env, mindsdb_env)
        except Exception as e:
            logger.error(e)
            res = {
                "n": 0,
                "writeErrors": [{"index": 0, "code": 0, "errmsg": str(e)}],
                "ok": 1,
            }
        return res

    def _insert_database(self, query, request_env, mindsdb_env):

        for doc in query["documents"]:
            if "_id" in doc:
                del doc["_id"]
            for field in ("name", "engine", "connection_args"):
                if field not in doc:
                    raise Exception(f"'{field}' must be specified")

            status = HandlerStatusResponse(success=False)
            try:
                handler = mindsdb_env["integration_controller"].create_tmp_handler(
                    name=doc['name'],
                    engine=doc["engine"],
                    connection_args=doc["connection_args"]
                )
                status = handler.check_connection()
            except Exception as e:
                status.error_message = str(e)

            if status.success is False:
                raise Exception(f"Can't connect to db: {status.error_message}")

            integration = mindsdb_env["integration_controller"].get(doc["name"])
            if integration is not None:
                raise Exception(f"Database '{doc['name']}' already exists.")

        for doc in query["documents"]:
            mindsdb_env["integration_controller"].add(
                doc["name"], doc["engine"], doc["connection_args"]
            )

    def _insert_model(self, query, request_env, mindsdb_env):
        predictors_columns = [
            "name",
            "status",
            "accuracy",
            "predict",
            "select_data_query",
            "training_options",
            "connection",
        ]

        if len(query["documents"]) != 1:
            raise Exception("Must be inserted just one predictor at time")

        for doc in query["documents"]:
            if "_id" in doc:
                del doc["_id"]

            action = doc.pop("action", "create").lower()

            bad_columns = [x for x in doc if x not in predictors_columns]
            if len(bad_columns) > 0:
                raise Exception(
                    f"Is no possible insert this columns to 'predictors' collection: {', '.join(bad_columns)}"
                )

            if "name" not in doc:
                raise Exception("Please, specify 'name' field")

            if "predict" not in doc:
                predict = None
                if action == "create":
                    raise Exception("Please, specify 'predict' field")
            else:
                predict = doc["predict"]
                if not isinstance(predict, list):
                    predict = [Identifier(x.strip()) for x in predict.split(",")]

            order_by = None
            group_by = None
            ts_settings = {}

            kwargs = doc.get("training_options", {})

            if "timeseries_settings" in kwargs:
                ts_settings = kwargs.pop("timeseries_settings")

                # mongo shell client sends int as float. need to convert it to int
                for key in ("window", "horizon"):
                    val = ts_settings.get(key)
                    if val is not None:
                        ts_settings[key] = int(val)

                if "order_by" in ts_settings:
                    order_by = ts_settings["order_by"]
                    if not isinstance(order_by, list):
                        order_by = [order_by]

                    order_by = [OrderBy(Identifier(x)) for x in order_by]
                if "group_by" in ts_settings:
                    group_by = [Identifier(x) for x in ts_settings.get("group_by", [])]

            using = dict(kwargs)

            select_data_query = doc.get("select_data_query")
            integration_name = None
            if "connection" in doc:
                integration_name = Identifier(doc["connection"])

            Class = CreatePredictor
            if action == "retrain":
                Class = RetrainPredictor
            elif action == "finetune":
                Class = FinetunePredictor

            create_predictor_ast = Class(
                name=Identifier(f"{request_env['database']}.{doc['name']}"),
                integration_name=integration_name,
                query_str=select_data_query,
                targets=predict,
                order_by=order_by,
                group_by=group_by,
                window=ts_settings.get("window"),
                horizon=ts_settings.get("horizon"),
                using=using,
            )

            run_sql_command(request_env, create_predictor_ast)

    def _insert_job(self, query, request_env, mindsdb_env):
        for doc in query["documents"]:

            query_str = doc["query"]
            # try parse as mongo
            parser = MongodbParser()
            try:
                mql = parser.from_string(query_str)

                method = mql.pipeline[0]["method"]
                if method == "find":
                    args = mql.pipeline[0]["args"]
                    projection = None
                    if len(args) > 1:
                        projection = args[1]
                    query = {
                        "find": mql.collection,
                        "filter": args[0],
                        "projection": projection,
                    }

                    for step in mql.pipeline[1:]:
                        if step["method"] == "limit":
                            query["limit"] = step["args"][0]
                        if step["method"] == "skip":
                            query["skip"] = step["args"][0]
                        if step["method"] == "sort":
                            query["sort"] = step["args"][0]
                    # TODO implement group modifiers
                    ast_query = find_to_ast(
                        query, request_env.get("database", default_project)
                    )

                    # to string
                    query_str = ast_query.to_string()
                elif method == "aggregate":
                    query = {
                        "aggregate": mql.collection,
                        "pipeline": mql.pipeline[0]["args"][0],
                    }
                    ast_query = aggregate_to_ast(
                        query, request_env.get("database", default_project)
                    )
                    query_str = ast_query.to_string()
            except Exception:
                # keep query
                pass

            repeat_str = doc.get("schedule_str").lower().lstrip("every ")

            ast_query = CreateJob(
                name=Identifier(doc["name"]),
                start_str=doc.get("start_at"),
                end_str=doc.get("end_at"),
                query_str=query_str,
                repeat_str=repeat_str,
            )
            run_sql_command(request_env, ast_query)

    def _insert_ml_engine(self, query, request_env, mindsdb_env):
        for doc in query["documents"]:

            ast_query = CreateMLEngine(
                name=Identifier(doc["name"]),
                handler=doc["handler"],
                params=doc.get("params"),
            )

            run_sql_command(request_env, ast_query)

    def _result(self, query, request_env, mindsdb_env):
        table = query["insert"]

        if table == "databases":
            self._insert_database(query, request_env, mindsdb_env)

        elif table in ["predictors", "models"]:
            self._insert_model(query, request_env, mindsdb_env)

        elif table == "jobs":
            self._insert_job(query, request_env, mindsdb_env)

        elif table == "ml_engines":
            self._insert_ml_engine(query, request_env, mindsdb_env)

        else:
            # regular insert

            df = pd.DataFrame(query["documents"])
            if "_id" in df.columns:
                df = df.drop("_id", axis=1)

            data = df.to_dict("split")
            values = []
            for row in data["data"]:
                values.append([Constant(i) for i in row])
            ast_query = Insert(
                table=Identifier(table),
                columns=[TableColumn(c) for c in data["columns"]],
                values=values,
            )

            run_sql_command(request_env, ast_query)

        result = {"n": len(query["documents"]), "ok": 1}

        return result


responder = Responce()
