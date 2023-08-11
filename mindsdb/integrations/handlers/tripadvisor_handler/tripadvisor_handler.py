import os
import datetime as dt

import time
from collections import defaultdict
from typing import Any

import pandas as pd
import tweepy

from mindsdb.utilities import log
from mindsdb.utilities.config import Config


from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.date_utils import parse_utc_date

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

from tripadvisor_api import TripAdvisorAPI
from tripadvisor_table import SearchLocationTable
from tripadvisor_api import TripAdvisorAPICall


class TripAdvisorHandler(APIHandler):
    """A class for handling connections and interactions with the TripAdvisor Content API.

    Attributes:
        api_key (str): The unique API key to access Tripadvisor content.
        api (TripAdvisorAPI): The `TripAdvisorAPI` object for checking the connection to the Twitter API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self._tables = {}

        self.connection_args = {}
        handler_config = Config().get("tripadvisor_handler", {})
        for k in ["api_key"]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f"TRIPADVISOR_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"TRIPADVISOR_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        tripAdvisor = SearchLocationTable(self)
        self._register_table("searchLocationTable", tripAdvisor)

    def connect(self, api_version=2):
        """Check the connection with TripAdvisor API"""

        if self.is_connected is True:
            return self.api

        self.api = TripAdvisorAPI(api_key=self.connection_args["api_key"])

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        """This function evaluates if the connection is alive and healthy"""
        response = StatusResponse(False)

        try:
            api = self.connect()

            # make a random http call with searching a location.
            #   it raises an error in case if auth is not success and returns not-found otherwise
            api.connectTripAdvisor()
            response.success = True

        except Exception as e:
            response.error_message = f"Error connecting to TripAdvisor api: {e}"
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def _register_table(self, table_name: str, table_class: Any):
        """It registers the data resource in memory."""
        self._tables[table_name] = table_class

    def call_twitter_api(
        self, method_name: str = None, params: dict = None, filters: list = None
    ):
        # method > table > columns
        expansions_map = {
            "search_recent_tweets": {
                "users": ["author_id", "in_reply_to_user_id"],
            },
            "search_all_tweets": {
                "users": ["author_id"],
            },
        }

        api = self.connect()
        method = getattr(api, method_name)

        # pagination handle

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

                if left > max_page_size:
                    params["max_results"] = max_page_size
                elif left < min_page_size:
                    params["max_results"] = min_page_size
                else:
                    params["max_results"] = left

            log.logger.debug(f">>>tripadvisor in: {method_name}({params})")
            resp = method(**params)

            if hasattr(resp, "includes"):
                for table, records in resp.includes.items():
                    includes[table].extend([r.data for r in records])

            if isinstance(resp.data, list):
                chunk = [r.data for r in resp.data]
            else:
                if isinstance(resp.data, dict):
                    data.append(resp.data)
                if hasattr(resp.data, "data") and isinstance(resp.data.data, dict):
                    data.append(resp.data.data)
                break

            # unwind columns
            for row in chunk:
                if "referenced_tweets" in row:
                    refs = row["referenced_tweets"]
                    if isinstance(refs, list) and len(refs) > 0:
                        if refs[0]["type"] == "replied_to":
                            row["in_reply_to_tweet_id"] = refs[0]["id"]
                        if refs[0]["type"] == "retweeted":
                            row["in_retweeted_to_tweet_id"] = refs[0]["id"]
                        if refs[0]["type"] == "quoted":
                            row["in_quote_to_tweet_id"] = refs[0]["id"]

            if filters:
                chunk = self._apply_filters(chunk, filters)

            # limit output
            if left is not None:
                chunk = chunk[:left]

            data.extend(chunk)
            # next page ?
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

    def call_tripadvisor_searchlocation_api(
        self, method_name: str = None, params: dict = None
    ) -> pd.DataFrame:
        # This will implement api base on the native query
        # By processing native query to convert it to api callable parameters
        if self.is_connected is False:
            self.connect()

        locations = self.api.makeRequest(TripAdvisorAPICall.SEARCH_LOCATION, **params)
        result = []

        for loc in locations:
            data = {
                "location_id": loc.location_id if loc.location_id else None,
                "name": loc.name if loc.name else None,
                "distance": loc.distance if loc.distance else None,
                "rating": loc.rating if loc.rating else None,
                "bearing": loc.bearing if loc.bearing else None,
                "street1": loc.address_obj.street1 if loc.address_obj.street1 else None,
                "street2": loc.address_obj.street2 if loc.address_obj.street2 else None,
                "city": loc.address_obj.city if loc.address_obj.city else None,
                "state": loc.address_obj.state if loc.address_obj.state else None,
                "country": loc.address_obj.country if loc.address_obj.country else None,
                "postalcode": loc.address_obj.postalcode
                if loc.address_obj.postalcode
                else None,
                "address_string": loc.address_obj.address_string
                if loc.address_obj.address_string
                else None,
                "phone": loc.address_obj.phone if loc.address_obj.phone else None,
                "latitude": loc.address_obj.latitude
                if loc.address_obj.latitude
                else None,
                "longitude": loc.address_obj.longitude
                if loc.address_obj.longitude
                else None,
            }
            result.append(data)

        result = pd.DataFrame(result)
        return result
