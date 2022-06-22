import re

import certifi
import pandas as pd
from pymongo import MongoClient

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from .utils.mongodb_render import MongodbRender


class MongoDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the MongoDB statements.
    """

    type = 'mongodb'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        connection_data = kwargs['connection_data']
        self.host = connection_data.get("host")
        self.port = int(connection_data.get("port") or 27017)
        self.user = connection_data.get("username")
        self.password = connection_data.get("password")
        self.database = connection_data.get('database')

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()


    def connect(self):
        kwargs = {}
        if isinstance(self.user, str) and len(self.user) > 0:
            kwargs['username'] = self.user

        if isinstance(self.password, str) and len(self.password) > 0:
            kwargs['password'] = self.password

        if re.match(r'/?.*tls=true', self.host.lower()):
            kwargs['tls'] = True

        if re.match(r'/?.*tls=false', self.host.lower()):
            kwargs['tls'] = False

        if re.match(r'.*.mongodb.net', self.host.lower()) and kwargs.get('tls', None) is None:
            kwargs['tlsCAFile'] = certifi.where()
            if kwargs.get('tls', None) is None:
                kwargs['tls'] = True


        connection = MongoClient(
            self.host,
            port=self.port,
            serverSelectionTimeoutMS=5000,
            **kwargs
        )
        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the database
        :return: success status and error message if error occurs
        """

        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            con = self.connect()
            con.server_info()
            result.success = True
        except Exception as e:
            log.error(f'Error connecting to MongoDB {self.database}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False

        return result


    def native_query(self, query) -> Response:

        """
        query = {
            'database': 'db_test', // optional
            'collection': 'fish', 
            'call': [   // call is sequence of methods
                {
                    'method': 'find',
                    'args': [{a:1}, {b:2}]
                },
                {
                    'method': 'sort',
                    'args': [{c:3}]
                },
            ]
        }        
      
        is the the same as mongo query:
            db_test.fish.find({a:1}, {b:2}).sort({c:3})

        returns the records from the current recordset
        """
        collection = query['collection']
        database = query.get('database', self.database)
        call = query['call']

        con = self.connect()

        try:

            cursor = con[database][collection]

            for step in call:
                fnc = getattr(cursor, step['method'])
                cursor = fnc(*step['args'])

            result = []
            for row in cursor:
                result.append(self.flatten(row))

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
            log.error(f'Error running query: {call} on {self.database}.{collection}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        return response

    def flatten(self, row, levels=2):
        # move sub-keys to upper level
        # TODO do we need it?

        add = {}
        del_keys = []
        for k, v in row.items():
            if isinstance(v, dict) and levels > 0:
                for k2, v2 in self.flatten(v, levels=levels - 1).items():
                    add[f'{k}.{k2}'] = v2
                del_keys.append(k)
        if add:
            row.update(add)
        for key in del_keys:
            del row[key]

        return row

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        renderer = MongodbRender()
        res = renderer.render(query)
        return self.native_query({
            'collection': res['collection'],
            'call': res['call']
        })


    def get_tables(self) -> Response:
        """
        Get a list with of collection in database
        """
        con = self.connect()
        collections = con[self.database].list_collection_names()
        df = pd.DataFrame([collections], columns=['table_name'])

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, collection) -> Response:
        """
        Use first row to detect columns
        """
        con = self.connect()
        record = con[self.database][collection].find_one()

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

