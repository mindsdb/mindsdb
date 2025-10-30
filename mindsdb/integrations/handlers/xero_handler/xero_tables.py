import pandas as pd
from abc import abstractmethod
from typing import List, Optional
import datetime

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from xero_python.accounting import AccountingApi


class XeroTable(APITable):
    """
    Base class for Xero API tables with common functionality
    """

    def __init__(self, handler):
        """
        Initialize the Xero table

        Args:
            handler: The Xero handler instance
        """
        super().__init__(handler)
        self.handler = handler

    def insert(self, query: ast.Insert) -> None:
        """Insert operations are not supported"""
        raise NotImplementedError("Insert operations are not supported for Xero tables")

    def update(self, query: ast.Update) -> None:
        """Update operations are not supported"""
        raise NotImplementedError("Update operations are not supported for Xero tables")

    def delete(self, query: ast.Delete) -> None:
        """Delete operations are not supported"""
        raise NotImplementedError("Delete operations are not supported for Xero tables")

    def _convert_response_to_dataframe(self, response_data: list) -> pd.DataFrame:
        """
        Convert API response to DataFrame

        Args:
            response_data: List of response objects

        Returns:
            pd.DataFrame: Flattened dataframe
        """
        if not response_data:
            return pd.DataFrame()

        # Convert objects to dictionaries
        rows = []
        for item in response_data:
            if hasattr(item, "to_dict"):
                rows.append(item.to_dict())
            elif isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(item.__dict__)

        df = pd.DataFrame(rows)
        return df

    @abstractmethod
    def get_columns(self) -> List[str]:
        """Get list of available columns"""
        pass

    @abstractmethod
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Execute SELECT query"""
        pass


class BudgetsTable(XeroTable):
    """Table for Xero Budgets"""

    def get_columns(self) -> List[str]:
        return [
            "budget_id",
            "budget_name",
            "description",
            "tracking_category_name",
            "tracking_option_name",
            "budget_line",
            "budget_amount",
            "updated_date_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch budgets from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch budgets
            budgets = api.get_budgets(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(budgets.budgets or [])
        except Exception as e:
            raise Exception(f"Failed to fetch budgets: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "budgets", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class ContactsTable(XeroTable):
    """Table for Xero Contacts"""

    def get_columns(self) -> List[str]:
        return [
            "contact_id",
            "contact_name",
            "email_address",
            "contact_status",
            "contact_type",
            "first_name",
            "last_name",
            "contact_number",
            "acc_number",
            "default_currency",
            "updated_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch contacts from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch contacts
            contacts = api.get_contacts(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(contacts.contacts or [])
        except Exception as e:
            raise Exception(f"Failed to fetch contacts: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "contacts", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class InvoicesTable(XeroTable):
    """Table for Xero Invoices"""

    def get_columns(self) -> List[str]:
        return [
            "invoice_id",
            "invoice_number",
            "reference",
            "status",
            "line_amount_types",
            "contact_name",
            "description",
            "invoice_date",
            "due_date",
            "updated_utc",
            "currency_code",
            "total",
            "amount_due",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch invoices from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch invoices
            invoices = api.get_invoices(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(invoices.invoices or [])
        except Exception as e:
            raise Exception(f"Failed to fetch invoices: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "invoices", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class ItemsTable(XeroTable):
    """Table for Xero Items"""

    def get_columns(self) -> List[str]:
        return [
            "item_id",
            "code",
            "description",
            "inventory_asset_account_code",
            "purchase_details",
            "sales_details",
            "is_tracked_as_inventory",
            "updated_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch items from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch items
            items = api.get_items(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(items.items or [])
        except Exception as e:
            raise Exception(f"Failed to fetch items: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "items", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class OverpaymentsTable(XeroTable):
    """Table for Xero Overpayments"""

    def get_columns(self) -> List[str]:
        return [
            "overpayment_id",
            "contact_name",
            "type",
            "status",
            "line_amount_types",
            "updated_utc",
            "currency_code",
            "overpayment_amount",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch overpayments from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch overpayments
            overpayments = api.get_overpayments(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(overpayments.overpayments or [])
        except Exception as e:
            raise Exception(f"Failed to fetch overpayments: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "overpayments", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class PaymentsTable(XeroTable):
    """Table for Xero Payments"""

    def get_columns(self) -> List[str]:
        return [
            "payment_id",
            "invoice_id",
            "account_code",
            "code",
            "amount",
            "payment_type",
            "status",
            "reference",
            "updated_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch payments from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch payments
            payments = api.get_payments(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(payments.payments or [])
        except Exception as e:
            raise Exception(f"Failed to fetch payments: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "payments", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class PurchaseOrdersTable(XeroTable):
    """Table for Xero Purchase Orders"""

    def get_columns(self) -> List[str]:
        return [
            "purchase_order_id",
            "purchase_order_number",
            "reference",
            "status",
            "contact_name",
            "delivery_date",
            "updated_utc",
            "currency_code",
            "total",
            "order_date",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch purchase orders from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch purchase orders
            purchase_orders = api.get_purchase_orders(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(purchase_orders.purchase_orders or [])
        except Exception as e:
            raise Exception(f"Failed to fetch purchase orders: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "purchase_orders", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class QuotesTable(XeroTable):
    """Table for Xero Quotes"""

    def get_columns(self) -> List[str]:
        return [
            "quote_id",
            "quote_number",
            "reference",
            "status",
            "contact_name",
            "quote_date",
            "expiry_date",
            "updated_utc",
            "currency_code",
            "total",
            "title",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch quotes from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch quotes
            quotes = api.get_quotes(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(quotes.quotes or [])
        except Exception as e:
            raise Exception(f"Failed to fetch quotes: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "quotes", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class RepeatingInvoicesTable(XeroTable):
    """Table for Xero Repeating Invoices"""

    def get_columns(self) -> List[str]:
        return [
            "repeating_invoice_id",
            "status",
            "contact_name",
            "type",
            "schedule",
            "reference",
            "updated_utc",
            "has_attachments",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch repeating invoices from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch repeating invoices
            repeating_invoices = api.get_repeating_invoices(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(repeating_invoices.repeating_invoices or [])
        except Exception as e:
            raise Exception(f"Failed to fetch repeating invoices: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "repeating_invoices", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()


class AccountsTable(XeroTable):
    """Table for Xero Chart of Accounts"""

    def get_columns(self) -> List[str]:
        return [
            "account_id",
            "code",
            "name",
            "type",
            "tax_type",
            "description",
            "enable_payments_to_account",
            "status",
            "updated_utc",
            "currency_code",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch accounts from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        try:
            # Fetch accounts
            accounts = api.get_accounts(tenant_id=self.handler.tenant_id)
            df = self._convert_response_to_dataframe(accounts.accounts or [])
        except Exception as e:
            raise Exception(f"Failed to fetch accounts: {str(e)}")

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "accounts", columns=self.get_columns()
        )
        executor = SELECTQueryExecutor(df, parser)
        return executor.execute()
