from typing import Any
import pykx as kx

import os
import ast
import re
import time
import pandas as pd

from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from collections import OrderedDict
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb_sql import parse_sql
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import ASTNode
from mindsdb_sql.planner.utils import query_traversal
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.integrations.libs.response import (
    HandlerResponse,
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
    HandlerStatusResponse
)

logger = log.getLogger(__name__)

class KDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the KDB statements.
    """
    
    name = 'kdb'
    
    def __init__(self, name, **kwargs):
        super().__init__(name)  
        self.parser = parse_sql
        self.dialect = 'postgresql'
        self.connection_data = kwargs.get('connection_data')
        self.renderer = SqlalchemyRender('postgres')
        
        self.connection = None
        self.is_connected = False
    
    def __del__(self):
        if self.is_connected is True:
            self.disconnect()
            
    def connect(self):
        
        if self.is_connected is True:
            return self.connection
        
        args = {
            'database': self.connection_data.get('database'),
        }
        
        print('Inside connect')
        # self.connection = 
        self.is_connected = True
        
        return self.connection
    
    def disconnect(self):
        if self.is_connected is False:
            return
        
        self.connection.close()
        self.is_connected = False
        
    def check_connection(self) -> HandlerStatusResponse:
        response = HandlerStatusResponse(False)
        need_to_close = self.is_connected is False
        
        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(
                f'Error connecting to KDB {self.connection_data["database"]}, {e}!'
            )
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response
    
    def native_query(self, query: str) -> HandlerResponse:
        need_to_close = self.is_connected is False
        
        connection = self.connect()
        cursor = connection.cursor()
        
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result, columns=[x[0] for x in cursor.description]    
                    ),
                )
            else:
                connection.commit()
                response = Response(RESPONSE_TYPE.OK)
        except Exception as e:
            logger.error(
                f'Error running query: {query} on {self.connection_data["database"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))
            
            cursor.close()
            if need_to_close is True:
                self.disconnect()
                
        return response
    
    def query(self, query: ASTNode) -> Response:
        
        query_str = self.renderer.get_string(query, with_fallback=True)
        return self.native_query(query_str)
    
    def get_tables(self) -> Response:
        q = 'SHOW TABLES;'
        result = self.native_query(q)
        df = result.data_frame
        result.data_frame = df.rename(columns={df.columns[0]: 'table_name'})
        return result
    
    def get_columns(self, table_name: str) -> HandlerResponse:
        query = f'DESCRIBE {table_name};'
        return self.native_query(query)
    
connection_args = OrderedDict(
    database = {
        'type': ARG_TYPE.STR,
        'description': 'The database file to read and write from. The special value :memory: (default) can be used to create an in-memory database.',
    }
)

connection_args_example = OrderedDict(database='db.kdb')