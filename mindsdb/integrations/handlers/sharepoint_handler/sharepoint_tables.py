import pandas as pd
from typing import Text, List, Dict, Any

from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)
from mindsdb.integrations.utilities.handlers.query_utilities.delete_query_utilities import (
    DELETEQueryParser,
    DELETEQueryExecutor,
)

from mindsdb.integrations.utilities.handlers.query_utilities.update_query_utilities import (
    UPDATEQueryParser,
    UPDATEQueryExecutor,
)

from mindsdb.integrations.utilities.handlers.query_utilities import INSERTQueryParser


class SitesTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Sharepoint Sites data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sharepoint Sites matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "sites", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        sites_df = pd.json_normalize(self.get_sites(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            sites_df, selected_columns, where_conditions, order_by_conditions
        )
        sites_df = select_statement_executor.execute_query()

        return sites_df

    def update(self, query: ast.Update) -> None:
        """Updates data in the Sharepoint "PUT /lists" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        sites_df = pd.json_normalize(self.get_sites())

        update_query_executor = UPDATEQueryExecutor(sites_df, where_conditions)

        sites_df = update_query_executor.execute_query()

        sites_ids = sites_df[["siteId"]].to_dict(orient="records")

        self.update_sites(sites_ids, values_to_update)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_sites(limit=1)).columns.tolist()

    def get_sites(self, **kwargs) -> List[Dict]:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        site_data = client.get_all_sites(**kwargs)
        return site_data

    def update_sites(self, site_ids: List[dict], values_to_update: dict) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.update_sites(site_ids, values_to_update)


class ListsTable(APITable):
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Sharepoint "POST /lists" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["displayName", "columns", "list", "siteId"],
            mandatory_columns=["displayName", "list", "siteId"],
            all_mandatory=False,
        )
        lists_data = insert_statement_parser.parse_query()
        self.create_lists(lists_data)

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Sharepoint lists data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sharepoint lists matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "lists", self.get_columns())
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        lists_df = pd.json_normalize(self.get_lists(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            lists_df, selected_columns, where_conditions, order_by_conditions
        )
        lists_df = select_statement_executor.execute_query()

        return lists_df

    def update(self, query: ast.Update) -> None:
        """Updates data in the Sharepoint "PUT /lists" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        lists_df = pd.json_normalize(self.get_lists())

        update_query_executor = UPDATEQueryExecutor(lists_df, where_conditions)

        lists_df = update_query_executor.execute_query()

        list_ids = lists_df[["id", "siteId"]].to_dict(orient="records")

        self.update_lists(list_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Sharepoint "DELETE /lists" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        lists_df = pd.json_normalize(self.get_lists())

        delete_query_executor = DELETEQueryExecutor(lists_df, where_conditions)

        lists_df = delete_query_executor.execute_query()

        list_ids = lists_df[["id", "siteId"]].to_dict(orient="records")
        self.delete_lists(list_ids)

    def get_lists(self, **kwargs) -> List[Dict]:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        lists_data = client.get_all_lists(**kwargs)
        return lists_data

    def delete_lists(self, list_ids: List[dict]) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.delete_lists(list_ids)

    def update_lists(self, list_ids: List[dict], values_to_update: dict) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.update_lists(list_ids, values_to_update)

    def create_lists(self, lists_data: List[Dict[Text, Any]]) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.create_lists(data=lists_data)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_lists(limit=1)).columns.tolist()


class SiteColumnsTable(APITable):
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Sharepoint "POST /columns" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=[
                "text",
                "name",
                "indexed",
                "enforceUniqueValues",
                "hidden",
                "siteId",
            ],
            mandatory_columns=["name", "siteId"],
            all_mandatory=False,
        )
        site_columns_data = insert_statement_parser.parse_query()
        self.create_site_columns(site_columns_data)

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Sharepoint columns data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sharepoint Columns matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query, "siteColumns", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        site_columns_df = pd.json_normalize(self.get_site_columns(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            site_columns_df,
            selected_columns,
            where_conditions,
            order_by_conditions,
        )
        site_columns_df = select_statement_executor.execute_query()

        return site_columns_df

    def update(self, query: ast.Update) -> None:
        """Updates data in the Sharepoint "PUT /columns" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        site_columns_df = pd.json_normalize(self.get_site_columns())

        update_query_executor = UPDATEQueryExecutor(site_columns_df, where_conditions)

        site_columns_df = update_query_executor.execute_query()

        site_columns_ids = site_columns_df[["id", "siteId"]].to_dict(orient="records")

        self.update_site_columns(site_columns_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Sharepoint "DELETE /columns" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        site_columns_df = pd.json_normalize(self.get_site_columns())

        delete_query_executor = DELETEQueryExecutor(site_columns_df, where_conditions)

        site_columns_df = delete_query_executor.execute_query()

        site_columns_ids = site_columns_df[["id", "siteId"]].to_dict(orient="records")
        self.delete_site_columns(site_columns_ids)

    def get_site_columns(self, **kwargs) -> List[Dict]:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        site_columns_data = client.get_all_site_columns(**kwargs)
        return site_columns_data

    def delete_site_columns(self, sharepoint_column_ids: List[dict]) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.delete_site_columns(sharepoint_column_ids)

    def update_site_columns(
        self, sharepoint_column_ids: List[dict], values_to_update: dict
    ) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.update_site_columns(sharepoint_column_ids, values_to_update)

    def create_site_columns(
        self, sharepoint_column_data: List[Dict[Text, Any]]
    ) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.create_site_columns(data=sharepoint_column_data)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_site_columns(limit=1)).columns.tolist()


class ListItemsTable(APITable):
    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the Sharepoint "POST /items" API endpoint.

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        insert_statement_parser = INSERTQueryParser(
            query,
            supported_columns=["fields", "listId", "siteId"],
            mandatory_columns=["listId", "siteId"],
            all_mandatory=False,
        )
        list_items_data = insert_statement_parser.parse_query()
        self.create_list_items(list_items_data)

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Pulls Sharepoint items data.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sharepoint items matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(
            query, "listItems", self.get_columns()
        )
        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        list_items_df = pd.json_normalize(self.get_list_items(limit=result_limit))
        select_statement_executor = SELECTQueryExecutor(
            list_items_df, selected_columns, where_conditions, order_by_conditions
        )
        list_items_df = select_statement_executor.execute_query()

        return list_items_df

    def update(self, query: ast.Update) -> None:
        """Updates data in the Sharepoint "PUT /items" API endpoint.

        Parameters
        ----------
        query : ast.Update
           Given SQL UPDATE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        update_statement_parser = UPDATEQueryParser(query)
        values_to_update, where_conditions = update_statement_parser.parse_query()

        list_items_df = pd.json_normalize(self.get_list_items())

        update_query_executor = UPDATEQueryExecutor(list_items_df, where_conditions)

        list_items_df = update_query_executor.execute_query()

        list_items_ids = list_items_df[["id", "siteId", "listId"]].to_dict(
            orient="records"
        )

        self.update_list_items(list_items_ids, values_to_update)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes data from the Sharepoint "DELETE /items" API endpoint.

        Parameters
        ----------
        query : ast.Delete
           Given SQL DELETE query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        delete_statement_parser = DELETEQueryParser(query)
        where_conditions = delete_statement_parser.parse_query()

        list_items_df = pd.json_normalize(self.get_list_items())

        delete_query_executor = DELETEQueryExecutor(list_items_df, where_conditions)

        list_items_df = delete_query_executor.execute_query()

        list_items_ids = list_items_df[["id", "siteId", "listId"]].to_dict(
            orient="records"
        )
        self.delete_list_items(list_items_ids)

    def get_list_items(self, **kwargs) -> List[Dict]:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        list_items_data = client.get_all_items(**kwargs)
        return list_items_data

    def delete_list_items(self, list_item_ids: List[dict]) -> None:
        if not self.handler.connnection.check_connection():
            self.handler.connect()
        client = self.handler.connection
        client.delete_items(list_item_ids)

    def update_list_items(
        self, list_items_ids: List[dict], values_to_update: dict
    ) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.update_items(item_dict=list_items_ids, values_to_update=values_to_update)

    def create_list_items(self, list_items_data: List[Dict[Text, Any]]) -> None:
        if not self.handler.connection.check_bearer_token_validity():
            self.handler.connect()
        client = self.handler.connection
        client.create_items(data=list_items_data)

    def get_columns(self) -> List[Text]:
        return pd.json_normalize(self.get_list_items(limit=1)).columns.tolist()
