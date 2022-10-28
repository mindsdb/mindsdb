# Set up Python package and API access
from simple_salesforce import Salesforce # The Salesforce function allows you to connect to the API (you will need API access and your Salesforce credentials)
import requests
import pandas as pd
from io import StringIO

from typing import Optional
from collections import OrderedDict

from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.utilities.log import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

class salesforce_handler(DatabaseHandler):
    """
     This handler handles connection and execution of Salesforce statements.
    """

    name = "salesforce"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler
        Args:
            name (str): name of particular handler instance
                        connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'salesforce'
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

        def __del__(self):
            if self.is_connected is True:
                self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        self.connection = simple_salesforce.client(
            'salesforce', 
            sf_username = Salesforce(username='SALESFORCE_API_USER', 
            sf_password = 'SALESFORCE_API_PASSWORD',
            sf_security_token='SALESFORCE_API_TOKEN',
            )
        )
        self.is_connected = True

        return self.connection

     def disconnect(self):
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

       try:
            self.connect()
            response.success = True
        except Exception as e:
            log.error(f'Error connecting to Salesforce with the given credentials, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in PostgreSQL
        :return: returns the records from the current recordset
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        with connection.cursor() as cur:
            try:
                result = connection.select_object_content(
                    sf_instance = ['https://oneappexchange.lightning.force.com/'],# Salesforce Instance URL
                    reportId = ['12345'], # add report id
                    export = ['?isdtp=p1&export=1&enc=UTF-8&xf=csv'],
                    response = requests.get(sfUrl, headers=sf.headers, cookies={'sid': sf.session_id})
                    download_report = response.content.decode('utf-8')
                    InputSerialization=ast.literal_eval(self.connection_data['input_serialization']),
                )

                '''
                Check all field names of the object
                '''
                descri=sf.UserInstall__c.describe()
                [field['name'] for field in descri['fields']]
                
                # writing SOQL query 
                results=sf.query_all("""
                    Select 
                    CreatedDate,
                    Listing__r.RecordTypeSnapshot__c,
                    Name,
                    Listing__r.ProviderName__c
                    from UserInstall__c
                    where CreatedDate=LAST_N_DAYS:7 
                    """)

            records = [dict(CreatedDate=rec['CreatedDate'], 
                Record_Type=rec['Listing__r']['RecordTypeSnapshot__c'],
                ProviderName=rec['Listing__r']['ProviderName__c'], 
                Name=rec['Name'] ) for rec in results['records']]
            df=pd.DataFrame(records)

            # perform calculations and aggregate dataset
            df['CreatedDate']=pd.to_datetime(df['CreatedDate']).dt.to_period('M')
            df['bucket']=df['ProviderName'].apply(lambda x: 'Labs' if x in ('Salesforce','Salesforce Labs') else 'Other')
            df=df.groupby(by=['CreatedDate','bucket'])['Name'].count().reset_index()

            df = pd.read_csv(io.StringIO(file_str))

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )

            except Exception as e:
                log.error(f'Error running query: {query} on {self.connection_data["key"]} in {self.connection_data["bucket"]}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(e)
                )
            
            if need_to_close is True:
                self.disconnect()

            return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        List all tabels in PostgreSQL without the system tables information_schema and pg_catalog
        """
        query = """
            SELECT
                table_schema,
                table_name,
                table_type
            FROM
                information_schema.tables
            WHERE
                table_schema NOT IN ('information_schema', 'pg_catalog')
                and table_type in ('BASE TABLE', 'VIEW')
        """
        return self.native_query(query)

    def get_columns(self, table_name):

        query = "SELECT * FROM S3Object LIMIT 5"
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': result.data_frame.dtypes
                }
            )
        )

        return response

        connection_args = OrderedDict(
            sf_instance ={
                'type': ARG_TYPE.STR,
                'description': 'The Salesforce instance URL.'
            },
            reportId={
                'type': ARG_TYPE.STR,
                'description': 'The Salesforce report ID x'
            },
            sf_instance={
                'type': ARG_TYPE.STR,
                'descriptipn': 'The Salesforce instance'
            },
            sf_username={
                'type': ARG_TYPE.STR,
                'description': 'The Salesforce username credentials' 
            },
            sf_password={
                'type': ARG_TYPE.STR,
                'description': 'The Salesforce user password credentials' 
            },
            sf_security_token={
                'type': ARG_TYPE.STR,
                'description': 'The Salesforce user security token'
            }
    
            connection_args_example = OrderedDict(
                sf_instance = 'https://oneappexchange.lightning.force.com/'# Salesforce Instance URL
                reportId = '12345' # add report id
                sf_username = Salesforce(username='SALESFORCE_API_USER', 
                sf_password = 'SALESFORCE_API_PASSWORD',
                sf_security_token='SALESFORCE_API_TOKEN',
            )