import re
import time
import threading

from bson import ObjectId
from mindsdb_sql_parser.ast.base import ASTNode
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure, ConfigurationError, InvalidURI
from typing import Text, List, Dict, Any, Union

from mindsdb.api.mongo.utilities.mongodb_query import MongoQuery
from mindsdb.api.mongo.utilities.mongodb_parser import MongodbParser
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
from .utils.mongodb_render import MongodbRender


logger = log.getLogger(__name__)


class MongoDBHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on MongoDB.
    """

    _SUBSCRIBE_SLEEP_INTERVAL = 0.5
    name = 'mongodb'

    def __init__(self, name: Text, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            kwargs: Arbitrary keyword arguments including the connection data.
        """
        super().__init__(name)
        connection_data = kwargs['connection_data']
        self.host = connection_data.get("host")
        self.port = int(connection_data.get("port") or 27017)
        self.user = connection_data.get("username")
        self.password = connection_data.get("password")
        self.database = connection_data.get('database')
        self.flatten_level = connection_data.get('flatten_level', 0)

        self.connection = None
        self.is_connected = False

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> MongoClient:
        """
        Establishes a connection to the MongoDB host.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            pymongo.MongoClient: A connection object to the MongoDB host.
        """
        kwargs = {}
        if isinstance(self.user, str) and len(self.user) > 0:
            kwargs['username'] = self.user

        if isinstance(self.password, str) and len(self.password) > 0:
            kwargs['password'] = self.password

        if re.match(r'/?.*tls=true', self.host.lower()):
            kwargs['tls'] = True

        if re.match(r'/?.*tls=false', self.host.lower()):
            kwargs['tls'] = False

        try:
            connection = MongoClient(
                self.host,
                port=self.port,
                **kwargs
            )
        except InvalidURI as invalid_uri_error:
            logger.error(f'Invalid URI provided for MongoDB connection: {invalid_uri_error}!')
            raise
        except ConfigurationError as config_error:
            logger.error(f'Configuration error connecting to MongoDB: {config_error}!')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to MongoDB: {unknown_error}!')
            raise

        # Get the database name from the connection if it's not provided.
        if self.database is None:
            self.database = connection.get_database().name

        self.is_connected = True
        self.connection = connection
        return self.connection

    def subscribe(self, stop_event: threading.Event, callback: callable, table_name: Text, columns: List = None, **kwargs: Any) -> None:
        """
        Subscribes to changes in a MongoDB collection and calls the provided callback function when changes occur.

        Args:
            stop_event (threading.Event): An event object to stop the subscription.
            callback (callable): The callback function to call when changes occur.
            table_name (Text): The name of the collection to subscribe to.
            columns (List): A list of columns to monitor for changes.
            kwargs: Arbitrary keyword arguments.
        """
        con = self.connect()
        cur = con[self.database][table_name].watch()

        while True:
            if stop_event.is_set():
                cur.close()
                return

            res = cur.try_next()
            if res is None:
                time.sleep(self._SUBSCRIBE_SLEEP_INTERVAL)
                continue

            _id = res['documentKey']['_id']
            if res['operationType'] == 'insert':
                if columns is not None:
                    updated_columns = set(res['fullDocument'].keys())
                    if not set(columns) & set(updated_columns):
                        # Do nothing.
                        continue

                callback(row=res['fullDocument'], key={'_id': _id})

            if res['operationType'] == 'update':
                if columns is not None:
                    updated_columns = set(res['updateDescription']['updatedFields'].keys())
                    if not set(columns) & set(updated_columns):
                        # Do nothing.
                        continue

                # Get the full document.
                full_doc = con[self.database][table_name].find_one(res['documentKey'])
                callback(row=full_doc, key={'_id': _id})

    def disconnect(self) -> None:
        """
        Closes the connection to the MongoDB host if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the MongoDB host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            con = self.connect()
            con.server_info()

            # Check if the database exists.
            if self.database not in con.list_database_names():
                raise ValueError(f'Database {self.database} not found!')

            response.success = True
        except (InvalidURI, ServerSelectionTimeoutError, OperationFailure, ConfigurationError, ValueError) as known_error:
            logger.error(f'Error connecting to MongoDB {self.database}, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Unknown error connecting to MongoDB {self.database}, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Union[Text, Dict, MongoQuery]) -> Response:
        """
        Executes a SQL query on the MongoDB host and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        if isinstance(query, str):
            query = MongodbParser().from_string(query)

        if isinstance(query, dict):
            # Fallback for the previous API.
            mquery = MongoQuery(query['collection'])

            for c in query['call']:
                mquery.add_step({
                    'method': c['method'],
                    'args': c['args']
                })

            query = mquery

        collection = query.collection
        database = self.database

        con = self.connect()

        # Check if the collection exists.
        if collection not in con[database].list_collection_names():
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f'Collection {collection} not found in database {database}!'
            )

        try:
            cursor = con[database][collection]

            for step in query.pipeline:
                fnc = getattr(cursor, step['method'])
                cursor = fnc(*step['args'])

            result = []
            if not isinstance(cursor, pymongo.results.UpdateResult):
                for row in cursor:
                    result.append(self.flatten(row, level=self.flatten_level))

            else:
                return Response(RESPONSE_TYPE.OK)

            if len(result) > 0:
                df = pd.DataFrame(result)
            else:
                columns = list(self.get_columns(collection).data_frame.Field)
                df = pd.DataFrame([], columns=columns)

            response = Response(
                RESPONSE_TYPE.TABLE,
                df
            )
        except Exception as e:
            logger.error(f'Error running query: {query} on {self.database}.{collection}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        return response

    def flatten(self, row: Dict, level: int = 0) -> Dict:
        """
        Flattens a nested dictionary to a single level.

        Args:
            row (Dict): The dictionary to flatten.
            level (int): The number of levels to flatten. If 0, the entire dictionary is flattened.

        Returns:
            Dict: The flattened dictionary.
        """
        add = {}
        del_keys = []
        edit_keys = {}

        for k, v in row.items():
            # Convert ObjectId to string.
            if isinstance(v, ObjectId):
                edit_keys[k] = str(v)
            if level > 0:
                if isinstance(v, dict):
                    for k2, v2 in self.flatten(v, level=level - 1).items():
                        add[f'{k}.{k2}'] = v2
                    del_keys.append(k)

        if add:
            row.update(add)
        for key in del_keys:
            del row[key]
        if edit_keys:
            row.update(edit_keys)

        return row

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the MongoDB host and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        renderer = MongodbRender()
        mquery = renderer.to_mongo_query(query)
        return self.native_query(mquery)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables (collections) in the MongoDB host.

        Returns:
            Response: A response object containing a list of tables (collections) in the MongoDB host.
        """
        con = self.connect()
        collections = con[self.database].list_collection_names()
        collections_ar = [
            [i] for i in collections
        ]
        df = pd.DataFrame(collections_ar, columns=['table_name'])

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column (field) details for a specified table (collection) in the MongoDB host.
        The first record in the collection is used to determine the column details.

        Args:
            table_name (Text): The name of the table (collection) for which to retrieve column (field) information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        con = self.connect()
        record = con[self.database][table_name].find_one()

        data = []
        if record is not None:
            record = self.flatten(record)

            for k, v in record.items():
                data.append([k, type(v).__name__])

        df = pd.DataFrame(data, columns=['Field', 'Type'])

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )
        return response
