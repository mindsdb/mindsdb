import os
import time
import pandas as pd
from collections import defaultdict

from pydantic import BaseModel, Extra
from notion_client import Client

from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

from .notion_table import (
    NotionBlocksTable,
    NotionCommentsTable,
    NotionDatabaseTable,
    NotionPagesTable,
)


class NotionHandlerArgs(BaseModel):
    target: str = None
    ft_api_info: dict = None
    ft_result_stats: dict = None
    runtime: str = None
    notion_api_token: str = None

    class Config:
        # for all args that are not expected, raise an error
        extra = Extra.forbid


class NotionHandler(APIHandler):
    name = "notion"

    def __init__(self, name: str, **kwargs):
        """constructor
        Args:
            name (str): the handler name
        """
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self.connection_args = {}

        handler_config = Config().get("notion_handler", {})
        token_name = ["notion_api_token", "api_token"]
        for k in token_name:
            if len(self.connection_args.keys()) > 0:
                self.connection_args[k] = self.connection_args.get(
                    list(self.connection_args.keys())[0]
                )
            if k in args:
                self.connection_args[k] = args[k]
            elif f"NOTION_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"NOTION_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

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
        api_token = self.connection_args["api_token"]
        notion = Client(auth=api_token)
        return notion

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            api = self.connect()

            response.success = True

        except Exception as e:
            response.error_message = (
                f"Error connecting to Notion api: {e}. Check notion_api_token"
            )
            log.logger.error(response.error_message)

        if response.success is True and not self.connection_args["api_token"]:
            try:
                api = self.connect()

            except Exception as e:
                keys = "notion_api_token"
                response.error_message = (
                    f"Error connecting to Notion api: {e}. Check" + ", ".join(keys)
                )
                log.logger.error(response.error_message)

                response.success = False

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def _apply_filters(self, data, filters):
        return data

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

        api = self.connect()
        service, query = method_name.split(".")
        if service in ["databases", "pages", "blocks", "comments"]:
            method = getattr(api, service)
            if query:
                method = getattr(method, query)

        count_results = None
        if "max_results" in params:
            count_results = params["max_results"]

        data = []
        includes = defaultdict(list)

        max_page_size = 100
        min_page_size = 10
        left = None

        limit_exec_time = time.time() + 60

        if filters:
            # if we have filters: do big page requests
            params["max_results"] = max_page_size

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

            log.logger.debug(f">>>notion in: {method_name}({params})")

            resp = method(**params)

            if hasattr(resp, "includes"):
                for table, records in resp.includes.items():
                    includes[table].extend([r.data for r in records])
            if resp.get("results"):
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
