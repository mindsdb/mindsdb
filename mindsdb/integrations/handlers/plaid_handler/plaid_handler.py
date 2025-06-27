import pandas as pd
from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler, FuncParser
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from datetime import datetime
import plaid
from plaid.api import plaid_api
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_balance_get_request_options import AccountsBalanceGetRequestOptions
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from .plaid_tables import BalanceTable, TransactionTable
from .utils import parse_transaction


PLAID_ENV = {
    'production': plaid.Environment.Production,
    'development': plaid.Environment.Development,
    'sandbox': plaid.Environment.Sandbox,
}

logger = log.getLogger(__name__)


class PlaidHandler(APIHandler):
    '''A class for handling connections and interactions with the Plaid API.

    Attributes:
        plaid_env (str): Enviroment used by user [ 'sandbox'(default) OR 'development' OR 'production' ].
        client_id (str): Your Plaid API client_id.
        secret (str): Your Plaid API secret
        access_token (str): The access token for the Plaid account.
    '''

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})

        self.plaid_config = plaid.Configuration(
            host=PLAID_ENV[args.get('plaid_env', 'sandbox')],
            api_key={
                'clientId': args.get('client_id'),
                'secret': args.get('secret')
            }
        )

        self.access_token = args.get('access_token')

        self.api = None
        self.is_connected = False

        balance = BalanceTable(self)
        transactions = TransactionTable(self)
        self._register_table('balance', balance)
        self._register_table('transactions', transactions)

    def connect(self):
        '''Authenticate with the Plaid API using the API keys and secrets stored in the `plaid_env`, `client_id`, `secret` , and `access_token`  attributes.'''  # noqa

        if self.is_connected is True:
            return self.api

        api_client = plaid.ApiClient(self.plaid_config)
        self.api = plaid_api.PlaidApi(api_client)
        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        '''It evaluates if the connection with Plaid API is alive and healthy.
        Returns:
            HandlerStatusResponse
        '''

        response = StatusResponse(False)

        try:
            api = self.connect()
            api.accounts_balance_get(AccountsBalanceGetRequest(
                access_token=self.access_token)
            )
            response.success = True

        except Exception as e:
            response.error_message = f'Error connecting to Plaid api: {e}. '
            logger.error(response.error_message)

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query_string: str = None):
        '''It parses any native statement string and acts upon it (for example, raw syntax commands).
        Args:
        query (Any): query in native format (str for sql databases,
            dict for mongo, api's json etc)
        Returns:
            HandlerResponse
        '''

        method_name, params = FuncParser().from_string(query_string)
        df = self.call_plaid_api(method_name, params)
        return Response(
            RESPONSE_TYPE.TABLE,
            data_frame=df
        )

    def call_plaid_api(self, method_name: str = None, params: dict = {}):
        '''Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind of query: SELECT, INSERT, DELETE, etc
        Returns:
            DataFrame
        '''

        result = pd.DataFrame()
        if method_name == 'get_balance':
            result = self.get_balance(params=params)
            result = BalanceTable(self).filter_columns(result=result)

        elif method_name == 'get_transactions':
            result = self.get_transactions(params=params)
            result = TransactionTable(self).filter_columns(result=result)

        return result

    def get_balance(self, params=None):
        '''Filters data from Plaid API's balance endpoint and returns a DataFrame with the required information.

        Args:
            params (dict, optional): A dictionary of options to be passed to the Plaid API.

        Returns:
            pandas.DataFrame: A DataFrame containing the filtered data.
        '''

        self.connect()
        if params.get('last_updated_datetime') is not None:
            options = AccountsBalanceGetRequestOptions(
                min_last_updated_datetime=datetime.strptime(
                    params.get('last_updated_datetime')
                )
            )

            response = self.api.accounts_balance_get(
                AccountsBalanceGetRequest(
                    access_token=self.access_token,
                    options=options
                )
            )
        else:
            response = self.api.accounts_balance_get(
                AccountsBalanceGetRequest(access_token=self.access_token)
            )

        messages = []
        for obj in response['accounts']:
            message_dict = {}
            for i in obj.to_dict().keys():
                if i.startswith('account_'):
                    message_dict[i] = obj[i]
                elif i == 'balances':
                    dict_obj = obj[i].to_dict()
                    for j in dict_obj.keys():
                        message_dict[f'balance_{j}'] = dict_obj[j]
                else:
                    message_dict[f'account_{i}'] = obj[i]
            messages.append(message_dict)
        df = pd.DataFrame(messages)

        return df

    def get_transactions(self, params={}):
        '''
        Filters data from Plaid API's transaction endpoint and returns a DataFrame with the required information.
        Args:
            params (dict, optional): A dictionary of options to be passed to the Plaid API.

        Returns:
            pandas.DataFrame: A DataFrame containing the filtered data.
        '''

        self.connect()
        if params.get('start_date') and params.get('end_date'):
            start_date = datetime.strptime(params.get('start_date'), '%Y-%m-%d').date()
            end_date = datetime.strptime(params.get('end_date'), '%Y-%m-%d').date()
        else:
            raise Exception('start_date and end_date is required in format YYYY-MM-DD ')

        request = TransactionsGetRequest(
            access_token=self.access_token,
            start_date=start_date,
            end_date=end_date,
            options=TransactionsGetRequestOptions()
        )

        response = self.api.transactions_get(request)
        transactions = parse_transaction(response['transactions'])

        # Manipulate the count and offset parameters to paginate
        # transactions and retrieve all available data
        while len(transactions) < response['total_transactions']:
            request = TransactionsGetRequest(
                access_token=self.access_token,
                start_date=start_date,
                end_date=end_date,
                options=TransactionsGetRequestOptions(
                    offset=len(transactions)
                )
            )
            response = self.api.transactions_get(request)
            transactions.extend(parse_transaction(response['transactions']))

        # Converting date column from str
        df = pd.DataFrame(transactions)
        for i in ['date', 'authorized_date']:
            df[i] = pd.to_datetime(df[i]).dt.date

        return df
