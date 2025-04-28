import os
from collections import OrderedDict

import pandas as pd
import google.cloud.storage as gcs

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response
)
from .google_cloud_storage_tables import GoogleCloudStorageBucketsTable
from mindsdb.utilities import log
logger = log.getLogger(__name__)

HANDLER_PATH = os.path.abspath(".")


class GoogleCloudStorageHandler(APIHandler):
    """
    A class for handling connections and interactions with the Google Cloud Storage API.
    """
    name = "google_cloud_storage"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        self.connection_data = kwargs.get("connection_data", {})
        self.keyfile = self.connection_data["keyfile"]
        self.client = None

        self.is_connected = False

        self.buckets = GoogleCloudStorageBucketsTable(self)
        self._register_table("buckets", self.buckets)

    def connect(self):
        """
        Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return self.client

        keyfile_path = os.path.join(HANDLER_PATH, self.keyfile)

        self.client = gcs.Client.from_service_account_json(keyfile_path)

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            self.connect()
            list(self.client.list_buckets())
            response.success = True
        except Exception as e:
            response.error_message = e
            logger.error(f"Error connecting to Google Cloud Storage API: {e}!")

        self.is_connected = response.success

        return response

    def native_query(self, query: str = None) -> Response:
        """
       Receive raw query and act upon it somehow.
       Args:
           query (Any): query in native format (str for sql databases,
               dict for mongo, api's json etc)
       Returns:
           HandlerResponse
       """
        func_name, params = FuncParser().from_string(query)

        df = self.call_application_api(func_name, params)

        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def list_buckets(self, params: dict = None) -> pd.DataFrame:
        """
        List all buckets in the Google Cloud Storage
        Returns:
            DataFrame
        """

        # Connect client
        self.connect()

        # Use the client to retrieve buckets
        buckets = list(self.client.list_buckets(project=params["user_project"], max_results=params["limit"]))

        # Create a DataFrame to hold bucket information
        bucket_data = []
        for bucket in buckets:
            bucket_data.append({
                "selfLink": [bucket.self_link],
                "id": [str(bucket.id)],
                "name": [str(bucket.name)],
                "projectNumber": [str(bucket.projectNumber)],
                "location": [str(bucket.location)],
                "storageClass": [str(bucket.storage_class)],
                "timeCreated": [str(bucket.time_created)],
                "updated": [str(bucket.updated)],
                "owner": [str(bucket.owner["entityId"])],
                "labels": [str(bucket.labels)]
            })

        # Convert the bucket data to a pandas DataFrame
        df = pd.DataFrame(bucket_data)

        return df

    def create_bucket(self, params: dict = None) -> pd.DataFrame:
        """
        Creates a bucket in Google Cloud Storage
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """

        # Connect client
        self.connect()

        new_bucket = self.client.bucket(bucket_name=params["name"], user_project=params["user_project"])

        for param, value in params:
            if param == "storageClass":
                new_bucket.storage_class(value=value)

        created_bucket = new_bucket.create(
            location=params["location"]
        )

        # Return information about the newly created bucket
        return pd.DataFrame({
            "name": [str(created_bucket.name)],
            "userProject": [str(created_bucket.user_project)],
            "location": [str(created_bucket.location)],
            "storageClass": [str(created_bucket.storage_class)]
        })

    def update_bucket(self, params: dict = None) -> pd.DataFrame:
        """
        Updates a bucket in Google Cloud Storage
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """

        # Connect client
        self.connect()

        # Get the existing bucket
        bucket = self.client.get_bucket(bucket_or_name=params["name"])

        for param, value in params:
            if param == "storageClass":
                bucket.storage_class(value)

        # Update the bucket with provided properties
        bucket.patch()

        # Return information about the updated bucket
        return pd.DataFrame({
            "name": [str(bucket.name)],
            "storageClass": [str(bucket.storage_class)],
        })

    def delete_bucket(self, params: dict = None) -> pd.DataFrame:
        """
        Deletes a bucket in Google Cloud Storage
        Args:
            params (dict): query parameters
        Returns:
            DataFrame
        """

        # Connect client
        self.connect()

        # Get the existing bucket
        bucket = self.client.get_bucket(bucket_or_name=params["name"])

        result = pd.DataFrame({
            "name": [str(bucket.name)],
        })

        # Delete the bucket
        bucket.delete()

        return result

    def call_application_api(self, func_name: str = None, params: dict = None) -> pd.DataFrame:
        """
        Call Google Cloud Storage API and map the data to pandas DataFrame
        Args:
            func_name (str): function name
            params (dict): query parameters
        Returns:
            DataFrame
        """

        if func_name is None:
            raise ValueError("Function name not provided")

        if func_name == "list_buckets":
            return self.list_buckets(params)
        elif func_name == "create_bucket":
            return self.create_bucket(params)
        elif func_name == "update_bucket":
            return self.update_bucket(params)
        elif func_name == "delete_bucket":
            return self.delete_bucket(params)
        else:
            raise NotImplementedError(f"Function \"{func_name}\" not supported")


connection_args = OrderedDict(
    keyfile={
        "type": ARG_TYPE.STR,
        "description": "Name of JSON keyfile used for Google Cloud Storage authentication. The keyfile is obtainable trough Google Cloud Storage itself.",
        "required": True,
        "label": "keyfile",
    },
)

connection_args_example = OrderedDict(
    keyfile="keyfile.json",
)
