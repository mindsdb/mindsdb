import json
import pandas as pd
import datetime as dt
from typing import Dict

from mindsdb.integrations.handlers.frappe_handler.frappe_tables import FrappeDocumentsTable
from mindsdb.integrations.handlers.frappe_handler.frappe_client import FrappeClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class FrappeHandler(APIHandler):
    """A class for handling connections and interactions with the Frappe API.

    Attributes:
        client (FrappeClient): The `FrappeClient` object for interacting with the Frappe API.
        is_connected (bool): Whether or not the API client is connected to Frappe.
        domain (str): Frappe domain to send API requests to.
        access_token (str): OAuth token to use for authentication.
    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.client = None
        self.is_connected = False

        args = kwargs.get('connection_data', {})
        if 'access_token' not in args:
            raise ValueError('"access_token" parameter required for authentication')
        if 'domain' not in args:
            raise ValueError('"domain" parameter required to connect to your Frappe instance')
        self.access_token = args['access_token']
        self.domain = args['domain']

        document_data = FrappeDocumentsTable(self)
        self._register_table('documents', document_data)
        self.connection_data = args

    def back_office_config(self):
        tools = {
            'register_sales_invoice': 'have to be used by assistant to register a sales invoice. Input is JSON object serialized as a string. Due date have to be passed in format: "yyyy-mm-dd".',
            'check_company_exists': 'useful to check the company is exist. Input is company',
            'check_expense_type': 'useful to check the expense_type is exist. Input is expense_type',
            'check_customer': 'useful to check the customer is exist. Input is customer',
            'check_item_code': 'have to be used to check the item code. Input is item_code',
        }
        return {
            'tools': tools,
        }

    def register_sales_invoice(self, data):
        """
          input is:
            {
              "due_date": "2023-05-31",
              "customer": "ksim",
              "items": [
                {
                  "name": "T-shirt--",
                  "description": "T-shirt",
                  "quantity": 1
                }
              ]
            }
        """
        invoice = json.loads(data)
        date = dt.datetime.strptime(invoice['due_date'], '%Y-%m-%d')
        if date <= dt.datetime.today():
            return 'Error: due_date have to be in the future'

        for item in invoice['items']:
            # rename column
            item['qty'] = item['quantity']
            del item['quantity']

            # add required fields
            item['uom'] = "Nos"
            item['conversion_factor'] = 1

            income_account = self.connection_data.get('income_account', "Sales Income - C8")
            item['income_account'] = income_account

        try:
            self.connect()
            self.client.post_document('Sales Invoice', invoice)
        except Exception as e:
            return f"Error: {e}"
        return "Success"

    def check_item_code(self, item_code):
        self.connect()
        result = self.client.get_documents('Item', filters=[['item_code', '=', item_code]])
        if len(result) == 1:
            return True
        return "Item doesn't exist: please use different name"

    def check_company_exists(self, name):
        self.connect()
        result = self.client.get_documents('Company', filters=[['name', '=', name]])
        if len(result) == 1:
            return True
        return "Company doesn't exist: please use different name"

    def check_expense_type(self, name):
        self.connect()
        result = self.client.get_documents('Expense Claim Type', filters=[['name', '=', name]])
        if len(result) == 1:
            return True
        return "Expense Claim Type doesn't exist: please use different name"

    def check_customer(self, name):
        self.connect()
        result = self.client.get_documents('Customer', filters=[['name', '=', name]])
        if len(result) == 1:
            return True
        return "Customer doesn't exist"

    def connect(self) -> FrappeClient:
        """Creates a new  API client if needed and sets it as the client to use for requests.

        Returns newly created Frappe API client, or current client if already set.
        """
        if self.is_connected is True and self.client:
            return self.client

        if self.domain and self.access_token:
            self.client = FrappeClient(self.domain, self.access_token)

        self.is_connected = True
        return self.client

    def check_connection(self) -> StatusResponse:
        """Checks connection to Frappe API by sending a ping request.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(False)

        try:
            client = self.connect()
            client.ping()
            response.success = True

        except Exception as e:
            logger.error(f'Error connecting to Frappe API: {e}!')
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query)
        return self.query(ast)

    def _document_to_dataframe_row(self, doctype, document: Dict) -> Dict:
        return {
            'doctype': doctype,
            'data': json.dumps(document)
        }

    def _get_document(self, params: Dict = None) -> pd.DataFrame:
        client = self.connect()
        doctype = params['doctype']
        document = client.get_document(doctype, params['name'])
        return pd.DataFrame.from_records([self._document_to_dataframe_row(doctype, document)])

    def _get_documents(self, params: Dict = None) -> pd.DataFrame:
        client = self.connect()
        doctype = params['doctype']
        limit = params.get('limit', None)
        filters = params.get('filters', None)
        fields = params.get('fields', None)
        documents = client.get_documents(doctype, limit=limit, fields=fields, filters=filters)
        return pd.DataFrame.from_records([self._document_to_dataframe_row(doctype, d) for d in documents])

    def _create_document(self, params: Dict = None) -> pd.DataFrame:
        client = self.connect()
        doctype = params['doctype']
        new_document = client.post_document(doctype, json.loads(params['data']))
        return pd.DataFrame.from_records([self._document_to_dataframe_row(doctype, new_document)])

    def call_frappe_api(self, method_name: str = None, params: Dict = None) -> pd.DataFrame:
        """Calls the Frappe API method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call (e.g. get_document)
            params (Dict): Params to pass to the API call
        """
        if method_name == 'get_documents':
            return self._get_documents(params)
        if method_name == 'get_document':
            return self._get_document(params)
        if method_name == 'create_document':
            return self._create_document(params)
        raise NotImplementedError('Method name {} not supported by Frappe API Handler'.format(method_name))
