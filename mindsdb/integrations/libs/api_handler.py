from mindsdb.integrations.libs.base import BaseHandler
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.utilities import log

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from pandas import DataFrame

from typing import List, Any
from typing import Tuple
from typing import Callable, ClassVar
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse


class APITable():

    def __init__(self, handler):
        self.handler = handler

    def select(self, query: ASTNode) -> HandlerResponse:
        raise NotImplementedError()

    def insert(self, query: ASTNode) -> HandlerResponse:
        raise NotImplementedError()

    def drop(self, query: ASTNode) -> HandlerResponse:
        raise NotImplementedError()

    def delete(self, query: ASTNode) -> HandlerResponse:
        raise NotImplementedError()

    def get_columns(self, query: ASTNode) -> HandlerResponse:
        raise NotImplementedError()
        
          
class APIHandler(BaseHandler):

    def __init__(self, name: str):
        """ constructor
        Args:
            name (str): the handler name
        """
        self.is_connected: bool = False
        
        self._tables = {}

    def _register_table(self, table_name: str, table_class: Any) -> bool:
        
        self._tables[table_name] = table_class

        return None
   
    
    def get_columns(self, table_name: str) -> StatusResponse:
        """ Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
        
        result = self._tables[table_name].get_columns()
        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [result[i]['COLUMN_NAME'] for column_name in self._tables[table_name]['columns'] ],
                        columns=['COLUMN_NAME']
                        
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            log.logger.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response

    def get_tables(self) -> StatusResponse:
        """ Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
        
        result = self._tables.keys()

        try:
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        [table_name for table_name in self._tables],
                        columns=['TABLE_NAME']
                        
                    )
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
            
        except Exception as e:
            log.logger.error(f'Error running while getting table {e} on ')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )



        return response

    
