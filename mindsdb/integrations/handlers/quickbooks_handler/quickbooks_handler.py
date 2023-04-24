import os
import pandas as pd
from qbosdk import QuickbooksOnlineSDK
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities.config import Config
from mindsdb.utilities import log

from .quickbooks_table import AccountsTable, PurchasesTable, BillPaymentsTable


class QuickbooksHandler(APIHandler):

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.connection_args = {}
        handler_config = Config().get('quickbooks_handler', {})
        for k in ['client_id', 'client_secret', 'refresh_token', 'realm_id',  'environment']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'QUICKBOOKS_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'QUICKBOOKS_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.quickbooks = None
        self.is_connected = False

        accountso = AccountsTable(self)
        self._register_table('accountso', accountso)
        purchases= PurchasesTable(self)
        self._register_table('purchases', purchases)
        bills_payments = BillPaymentsTable(self)
        self._register_table('bills_payments', bills_payments)

    def connect(self):
        if self.is_connected is True:
            return self.quickbooks

        self.quickbooks = QuickbooksOnlineSDK(
            client_id=self.connection_args['client_id'],
            client_secret=self.connection_args['client_secret'],
            realm_id=self.connection_args['realm_id'],
            refresh_token=self.connection_args['refresh_token'],
            environment=self.connection_args['environment']
        )
        self.is_connected = True
        return self.quickbooks

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            quickbooks = self.connect()
            quickbooks.accounts.get()
            log.logger.info(quickbooks.accounts.get())
            print('Connected to Quickbooks API')
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Quickbooks API: {e}. '
            log.logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            print('Disconnected from Quickbooks API')
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        # Add your native query parsing and execution here.
        pass
