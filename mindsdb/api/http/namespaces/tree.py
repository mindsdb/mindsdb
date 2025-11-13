import inspect
from collections import defaultdict

from flask import current_app as ca, request
from flask_restx import Resource

from mindsdb.api.http.utils import http_error
from mindsdb.api.http.namespaces.configs.tree import ns_conf
from mindsdb.metrics.metrics import api_endpoint_metrics


@ns_conf.route("/")
class GetRoot(Resource):
    @ns_conf.doc("get_tree_root")
    @api_endpoint_metrics("GET", "/tree")
    def get(self):
        databases = ca.database_controller.get_list()
        result = [
            {
                "name": x["name"],
                "class": "db",
                "type": x["type"],
                "engine": x["engine"],
                "deletable": x["deletable"],
                "visible": x["visible"],
            }
            for x in databases
        ]
        return result


@ns_conf.route("/<db_name>")
@ns_conf.param("db_name", "Name of the database")
class GetLeaf(Resource):
    @ns_conf.doc("get_tree_leaf")
    @api_endpoint_metrics("GET", "/tree/database")
    def get(self, db_name):
        with_schemas = request.args.get("all_schemas")
        if isinstance(with_schemas, str):
            with_schemas = with_schemas.lower() in ("1", "true")
        else:
            with_schemas = False
        db_name = db_name.lower()
        databases = ca.database_controller.get_dict()
        if db_name not in databases:
            return http_error(400, "Error", f"There is no element with name '{db_name}'")
        db = databases[db_name]
        if db["type"] == "project":
            project = ca.database_controller.get_project(db_name)
            tables = project.get_tables()
            tables = [
                {
                    "name": key,
                    "schema": None,
                    "class": "table",
                    "type": val["type"],
                    "engine": val.get("engine"),
                    "deletable": val.get("deletable"),
                }
                for key, val in tables.items()
            ]

            jobs = ca.jobs_controller.get_list(db_name)
            tables = tables + [
                {"name": job["name"], "schema": None, "class": "job", "type": "job", "engine": "job", "deletable": True}
                for job in jobs
            ]
        elif db["type"] == "data":
            handler = ca.integration_controller.get_data_handler(db_name)
            if "all" in inspect.signature(handler.get_tables).parameters:
                response = handler.get_tables(all=with_schemas)
            else:
                response = handler.get_tables()
            if response.type != "table":
                return []
            table_types = {"BASE TABLE": "table", "VIEW": "view"}
            tables = response.data_frame.to_dict(orient="records")

            schemas = defaultdict(list)

            for table_meta in tables:
                table_meta = {key.lower(): val for key, val in table_meta.items()}
                schama = table_meta.get("table_schema")
                schemas[schama].append(
                    {
                        "name": table_meta["table_name"],
                        "class": "table",
                        "type": table_types.get(table_meta.get("table_type")),
                        "engine": None,
                        "deletable": False,
                    }
                )
            if len(schemas) == 1 and list(schemas.keys())[0] is None:
                tables = schemas[None]
            else:
                tables = [
                    {"name": key, "class": "schema", "deletable": False, "children": val}
                    for key, val in schemas.items()
                ]
        elif db["type"] == "system":
            system_db = ca.database_controller.get_system_db(db_name)
            tables = system_db.get_tree_tables()
            tables = [
                {
                    "name": table.name,
                    "class": table.kind,
                    "type": "system view",
                    "engine": None,
                    "deletable": table.deletable,
                }
                for table in tables.values()
            ]
        return tables
