import time
import pandas as pd
from collections import defaultdict

from notion_client import Client

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse,
    RESPONSE_TYPE,
)
from .notion_table import (
    NotionBlocksTable,
    NotionCommentsTable,
    NotionDatabaseTable,
    NotionPagesTable,
)

logger = log.getLogger(__name__)


class NotionHandler(APIHandler):
    name = "notion"

    def __init__(self, name: str, **kwargs):
        """constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)

        self.connection_args = kwargs.get("connection_data", {})
        self.key = "api_token"

        # set the api token from the args in create query
        if self.key in self.connection_args:
            self.connection_args[self.key] = self.connection_args[self.key]

        self.api = None
        self.is_connected = False

        notion_database_data = NotionDatabaseTable(self)
        notion_pages_data = NotionPagesTable(self)
        notion_comments_data = NotionCommentsTable(self)
        notion_blocks_data = NotionBlocksTable(self)

        self._register_table("database", notion_database_data)
        self._register_table("pages", notion_pages_data)
        self._register_table("blocks", notion_blocks_data)
        self._register_table("comments", notion_comments_data)

    def connect(self, args=None, **kwargs):
        api_token = self.connection_args[self.key]
        notion = Client(auth=api_token)
        self.is_connected = True
        return notion

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True

        except Exception as e:
            response.error_message = (
                f"Error connecting to Notion api: {e}. Check api_token"
            )
            logger.error(response.error_message)
            response.success = False

        if response.success is False:
            self.is_connected = False

        return response

    def native_query(self, query: str = None) -> HandlerResponse:
        method_name, param = query.split("(")
        params = dict()
        # parse the query as a python function
        for map in param.strip(")").split(","):
            if map:
                k, v = map.split("=")
                params[k] = v

        df = self.call_notion_api(method_name, params)
        return HandlerResponse(RESPONSE_TYPE.TABLE, data_frame=df)

    def _apply_filters(self, data, filters):
        if not filters:
            return data

        data2 = []
        for row in data:
            add = False
            for op, key, value in filters:
                value2 = row.get(key)
                if isinstance(value, int):
                    value = str(value)

                if op in ("!=", "<>"):
                    if value == value2:
                        break
                elif op in ("==", "="):
                    if value != value2:
                        break
                elif op == "in":
                    if not isinstance(value, list):
                        value = [value]
                    if value2 not in value:
                        break
                elif op == "not in":
                    if not isinstance(value, list):
                        value = [value]
                    if value2 in value:
                        break
                else:
                    raise NotImplementedError(f"Unknown filter: {op}")
                add = True
            if add:
                data2.append(row)
        return data2

    def call_notion_api(
        self, method_name: str = None, params: dict = None, filters: list = None
    ):
        # method > table > columns
        expansions_map = {
            "database": {
                "page": ["id", "url", "properties"],
            },
            "page": {
                "properties": ["Name"],
            },
        }

        self.api = self.connect()
        # use the service as the resource to query(database, page, block, comment)
        # and query as the type of method(retrieve, list, query)
        parts = method_name.split(".")
        if len(parts) == 2:
            service, query = parts
            if service in ["databases", "pages", "blocks", "comments"]:
                method = getattr(self.api, service)
                method = getattr(method, query)
        else:
            service, children, query = parts
            if service in ["blocks"]:
                method = getattr(self.api, service)
                method = getattr(method, children)
                method = getattr(method, query)

        count_results = None
        data = []
        includes = defaultdict(list)

        max_page_size = 100
        left = None

        limit_exec_time = time.time() + 60

        if filters:
            # if we have filters: do big page requests
            params["max_results"] = max_page_size

        chunk = []
        while True:
            if time.time() > limit_exec_time:
                raise RuntimeError("Handler request timeout error")

            if count_results is not None:
                left = count_results - len(data)
                if left == 0:
                    break
                elif left < 0:
                    # got more results that we need
                    data = data[:left]
                    break

            logger.debug(f">>>notion in: {method_name}({params})")

            resp = method(**params)

            if hasattr(resp, "includes"):
                for table, records in resp.includes.items():
                    includes[table].extend([r.data for r in records])
            if resp.get("results"):
                # database and comment api has list of results
                if isinstance(resp["results"], list):
                    chunk = [r for r in resp["results"]]
            else:
                if resp.get("object") in ["page", "block"]:
                    chunk = [resp]

            if filters:
                chunk = self._apply_filters(chunk, filters)

            # limit output
            if left is not None:
                chunk = chunk[:left]

            data.extend(chunk)
            if (
                count_results is not None
                and hasattr(resp, "meta")
                and "next_token" in resp.meta
            ):
                params["next_token"] = resp.meta["next_token"]
            else:
                break

        df = pd.DataFrame(data)

        # enrich
        expansions = expansions_map.get(method_name)
        if expansions is not None:
            for table, records in includes.items():
                df_ref = pd.DataFrame(records)

                if table not in expansions:
                    continue

                for col_id in expansions[table]:
                    col = col_id[:-3]  # cut _id
                    if col_id not in df.columns:
                        continue

                    col_map = {
                        col_ref: f"{col}_{col_ref}" for col_ref in df_ref.columns
                    }
                    df_ref2 = df_ref.rename(columns=col_map)
                    df_ref2 = df_ref2.drop_duplicates(col_id)

                    df = df.merge(df_ref2, on=col_id, how="left")

        return df
