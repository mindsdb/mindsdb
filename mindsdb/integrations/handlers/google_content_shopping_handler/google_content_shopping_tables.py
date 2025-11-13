import pandas as pd
from mindsdb_sql_parser import ast
from pandas import DataFrame

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.date_utils import parse_utc_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class AccountsTable(APITable):
    """
    Table class for the Google Content API for Shopping Accounts table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Lists the sub-accounts in your Merchant Center account.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        accepted_params = ['view', 'label', 'name']
        for op, arg1, arg2 in conditions:
            if arg1 == 'accountId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            if arg1 in accepted_params:
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['maxResults'] = query.limit.value

        # Get the accounts from the API.
        accounts = self.handler. \
            call_application_api(method_name='get_accounts', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(accounts) == 0:
            accounts = pd.DataFrame([], columns=selected_columns)
        else:
            accounts.columns = self.get_columns()
            for col in set(accounts.columns).difference(set(selected_columns)):
                accounts = accounts.drop(col, axis=1)
        return accounts

    def delete(self, query: ast.Delete):
        """
        Deletes accounts from your Merchant Center account.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'accountId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
            elif arg1 == 'force':
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name='delete_accounts', params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'name',
            'kind',
            'websiteUrl',
            'adultContent',
            'sellerId',
            'users',
            'id',
            'youtubeChannelLinks',
            'googleMyBusinessLink',
            'businessInformation',
            'automaticImprovements',
            'adsLinks',
            'cssId',
            'labelIds',
            'accountManagement',
            'automaticLabelIds',
            'conversionSettings'
        ]


class OrdersTable(APITable):
    """
    Table class for the Google Content API for Shopping Orders table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Lists the orders in your Merchant Center account.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        accepted_params = ['statuses', 'acknowledged']
        for op, arg1, arg2 in conditions:
            if arg1 == 'orderId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            if arg1 == 'placedDateStart' or arg1 == 'placedDateEnd':
                if op != '=':
                    raise NotImplementedError
                params[arg1] = parse_utc_date(arg2)
            if arg1 in accepted_params:
                params[arg1] = parse_utc_date(arg2)
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        if query.order_by is not None:
            if query.order_by[0].value == 'placedDate':
                if query.order_by[1].value == 'ASC':
                    params['orderBy'] = 'placedDateAsc'
                else:
                    params['orderBy'] = 'placedDateDesc'
                raise NotImplementedError
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['maxResults'] = query.limit.value

        # Get the orders from the API.
        orders = self.handler. \
            call_application_api(method_name='get_orders', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(orders) == 0:
            orders = pd.DataFrame([], columns=selected_columns)
        else:
            orders.columns = self.get_columns()
            for col in set(orders.columns).difference(set(selected_columns)):
                orders = orders.drop(col, axis=1)
        return orders

    def delete(self, query: ast.Delete):
        """
        Deletes orders in your Merchant Center account.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'orderId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        # Delete the events in the Google Calendar API.
        self.handler.call_application_api(method_name='delete_orders', params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'id',
            'merchantId',
            'merchantOrderId',
            'kind',
            'lineItems',
            'status',
            'paymentStatus',
            'acknowledged',
            'placedDate',
            'deliveryDetails',
            'customer',
            'shippingCost',
            'shippingCostTax',
            'refunds',
            'shipments',
            'billingAddress',
            'promotions',
            'taxCollector',
            'netPriceAmount',
            'netTaxAmount',
            'pickupDetails',
            'annotations'
        ]


class ProductsTable(APITable):
    """
    Table class for the Google Content API for Shopping Products table.
    """

    def select(self, query: ast.Select) -> DataFrame:
        """
        Lists the products in your Merchant Center account.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'productId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        if query.limit is not None:
            params['maxResults'] = query.limit.value

        # Get the products from the API.
        products = self.handler. \
            call_application_api(method_name='get_products', params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(products) == 0:
            products = pd.DataFrame([], columns=selected_columns)
        else:
            products.columns = self.get_columns()
            for col in set(products.columns).difference(set(selected_columns)):
                products = products.drop(col, axis=1)
        return products

    def update(self, query: ast.Update):
        """
        Updates products in your Merchant Center account.

        Args:
            query (ast.Update): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        params = {}
        values = query.values[0]
        # Get the event data from the values.
        accepted_params = self.get_columns()
        for col, val in zip(query.update_columns, values):
            if col in accepted_params:
                params[col] = val
            else:
                raise NotImplementedError

        params['updateMask'] = ','.join(params.keys())
        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.

        for op, arg1, arg2 in conditions:
            if arg1 == 'productId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            else:
                raise NotImplementedError

        # Update the products in the Google Merchant Center API.
        self.handler.call_application_api(method_name='update_products', params=params)

    def delete(self, query: ast.Delete):
        """
        Deletes products in your Merchant Center account.

        Args:
            query (ast.Delete): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)
        # Get the start and end times from the conditions.
        params = {}
        for op, arg1, arg2 in conditions:
            if arg1 == 'productId':
                if op == '=':
                    params[arg1] = arg2
                elif op == '>':
                    params['startId'] = arg2
                elif op == '<':
                    params['endId'] = arg2
                else:
                    raise NotImplementedError
            elif arg1 == 'feedId':
                if op != '=':
                    raise NotImplementedError
                params[arg1] = arg2
            else:
                raise NotImplementedError

        # Delete the products in the Google Merchant Center API.
        self.handler.call_application_api(method_name='delete_products', params=params)

    def get_columns(self) -> list:
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            'id',
            'offerId',
            'title',
            'description',
            'link',
            'imageLink',
            'contentLanguage',
            'targetCountry',
            'channel',
            'channelExclusivity',
            'price',
            'salePrice',
            'salePriceEffectiveDate',
            'gtin',
            'mpn',
            'brand',
            'condition',
            'adult',
            'multipack',
            'isBundle',
            'energyEfficiencyClass',
            'minEnergyEfficiencyClass',
            'maxEnergyEfficiencyClass',
            'ageGroup',
            'color',
            'expirationDate',
            'disclosureDate',
            'availability',
            'source'
        ]
