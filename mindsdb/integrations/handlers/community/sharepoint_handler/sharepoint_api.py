import ast
from datetime import datetime, timezone
from typing import Text, List, Dict, Any

from mindsdb.integrations.handlers.sharepoint_handler.utils import (
    bearer_token_request,
    get_an_entity,
    delete_an_entity,
    update_an_entity,
    create_an_entity,
)


class SharepointAPI:
    def __init__(
        self, client_id: str = None, client_secret: str = None, tenant_id: str = None
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.bearer_token = None
        self.is_connected = False
        self.expiration_time = datetime.now(timezone.utc).timestamp()

    def get_bearer_token(self) -> None:
        """
        Generates new bearer token for the credentials

        Returns
        None
        """
        response = bearer_token_request(
            client_id=self.client_id,
            tenant_id=self.tenant_id,
            client_secret=self.client_secret,
        )
        self.bearer_token = response["access_token"]
        self.expiration_time = int(response["expires_on"])
        self.is_connected = True

    def check_bearer_token_validity(self) -> bool:
        """
        Provides information whether a valid bearer token is available or not. Returns true if available
        otherwise false

        Returns
        bool
        """
        if (
            self.is_connected
            and datetime.now(timezone.utc).astimezone().timestamp()
            < self.expiration_time
        ):
            return True
        else:
            return False

    def disconnect(self) -> None:
        """
        Removes bearer token from the sharepoint API class (makes it null)

        Returns
        None
        """
        self.bearer_token = None
        self.is_connected = False

    def get_all_sites(self, limit: int = None) -> List[Dict[Text, Any]]:
        """
        Gets all sites associated with the account

        limit: limits the number of site information to be returned

        Returns
        response: metadata information corresponding to all sites
        """
        url = "https://graph.microsoft.com/v1.0/sites?search=*"
        response = get_an_entity(url=url, bearer_token=self.bearer_token)
        if limit:
            response = response[:limit]
        return response

    def update_sites(
        self, site_dict: List[Dict[Text, Text]], values_to_update: Dict[Text, Any]
    ) -> None:
        """
        Updates the given sites (site_dict) with the provided values (values_to_update)
        Calls the function update_a_site for every site
        site_dict: A dictionary containing site ids of the sites which are to be updated
        values_to_update: a dictionary which will be used to update the fields of the sites

        Returns
        None
        """
        for site_entry in site_dict:
            self.update_a_site(
                site_id=site_entry["siteId"],
                values_to_update=values_to_update,
            )

    def update_a_site(self, site_id: str, values_to_update: Dict[Text, Any]) -> None:
        """
        Updates a site with given values
        site_id: GUID of the site
        values_to_update: a dictionary values which will be used to update the properties of the site

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/"
        update_an_entity(
            url=url, values_to_update=values_to_update, bearer_token=self.bearer_token
        )

    def get_lists_by_site(
        self, site_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        """
        Gets lists' information corresponding to a site

        site_id:  GUID of a site
        limit: limits the number of lists for which information is returned

        Returns
        response: metadata information/ fields corresponding to lists of the site
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        response = get_an_entity(url=url, bearer_token=self.bearer_token)
        if limit:
            response = response[:limit]
        return response

    def get_all_lists(self, limit: int = None) -> List[Dict[Text, Any]]:
        """
        Gets all the lists' information assocaited with the account

        limit: puts a limit to the number of lists returned

        Returns
        response: returns metadata information regarding all the lists that have been made using that account
        """
        sites = self.get_all_sites()
        lists = []
        for site in sites:
            for list_dict in self.get_lists_by_site(site_id=site["id"].split(",")[1]):
                list_dict["siteName"] = site["name"]
                list_dict["siteId"] = site["id"].split(",")[1]
                lists.append(list_dict)
        if limit:
            lists = lists[:limit]
        return lists

    def delete_lists(self, list_dict: List[Dict[Text, Any]]) -> None:
        """
        Deletes lists for the given site ID and list ID

        list_dict: a dictionary values containing the list IDs which are to be deleted and
        their corresponding site IDs

        Returns
        None
        """
        for list_entry in list_dict:
            self.delete_a_list(site_id=list_entry["siteId"], list_id=list_entry["id"])

    def delete_a_list(self, site_id: str, list_id: str) -> None:
        """
        Deletes a list, given its list ID and its site ID

        site_id: GUID of the site in which the list is present
        list_id: GUID of the list which is to be deleted

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
        delete_an_entity(url=url, bearer_token=self.bearer_token)

    def update_lists(
        self, list_dict: List[Dict[Text, Text]], values_to_update: Dict[Text, Any]
    ) -> None:
        """
        Updates the given lists (list_dict) with the provided values (values_to_update)
        Calls the function update_a_list for every list
        list_dict: A dictionary containing ids of the list which are to be updated and also their site IDs
        values_to_update: a dictionary which will be used to update the fields of the lists

        Returns
        None
        """
        for list_entry in list_dict:
            self.update_a_list(
                site_id=list_entry["siteId"],
                list_id=list_entry["id"],
                values_to_update=values_to_update,
            )

    def update_a_list(
        self, site_id: str, list_id: str, values_to_update: Dict[Text, Any]
    ) -> None:
        """
        Updates a list with given values
        list_id: GUID of the list
        site_id: GUID of the site in which the list is present
        values_to_update: a dictionary values which will be used to update the properties of the list

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/"
        update_an_entity(
            url=url, bearer_token=self.bearer_token, values_to_update=values_to_update
        )

    def create_lists(self, data: List[Dict[Text, Any]]) -> None:
        """
        Creates lists with the information provided in the data parameter
        calls create_a_list for each entry of list metadata dictionary

        data: parameter which contains information such as the site IDs where the lists would be created
        and their metadata information which will be used to create them

        Returns
        None
        """
        for entry in data:
            self.create_a_list(
                site_id=entry["siteId"],
                column=entry.get("column"),
                display_name=entry["displayName"],
                list_template=entry["list"],
            )

    def create_a_list(
        self, site_id: str, list_template: str, display_name: str, column: str = None
    ) -> None:
        """
        Creates a list with metadata information provided in the params

        site_id: GUID of the site where the list is to be created
        list_template: a string which contains the list template information (type of list)
            eg.- "{'template': 'documentLibrary'}"
        display_name: the display name of the given list, which will be displayed in the site
        column: specifies the list of columns that should be created for the list
            eg.- "[{'name': 'Author', 'text': { }},{'name': 'PageCount', 'number': { }}]"

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/"
        payload = {}
        if column:
            column = ast.literal_eval(column)
            payload["column"] = column
        payload["displayName"] = display_name
        payload["list"] = ast.literal_eval(list_template)
        create_an_entity(url=url, payload=payload, bearer_token=self.bearer_token)

    def get_site_columns_by_site(
        self, site_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        """
        Gets columns' information corresponding to a site

        site_id:  GUID of a site
        limit: limits the number of columns for which information is returned

        Returns
        response: metadata information/ fields corresponding to columns of the site
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/"
        response = get_an_entity(url=url, bearer_token=self.bearer_token)
        if limit:
            response = response[:limit]
        return response

    def get_all_site_columns(self, limit: int = None) -> List[Dict[Text, Any]]:
        """
        Gets all the columns' information associated with the account

        limit: puts a limit to the number of columns returned

        Returns
        response: returns metadata information regarding all the columns that have been made using that account
        """
        sites = self.get_all_sites()
        site_columns = []
        for site in sites:
            for site_column_dict in self.get_site_columns_by_site(
                site_id=site["id"].split(",")[1]
            ):
                site_column_dict["siteName"] = site["name"]
                site_column_dict["siteId"] = site["id"].split(",")[1]
                site_columns.append(site_column_dict)
        if limit:
            site_columns = site_columns[:limit]
        return site_columns

    def update_site_columns(
        self,
        site_column_dict: List[Dict[Text, Text]],
        values_to_update: Dict[Text, Any],
    ) -> None:
        """
        Updates the given columns (site_column_dict) with the provided values (values_to_update)
        Calls the function update_a_site_column for every column

        site_column_dict: A dictionary containing ids of the column which are to be updated and
        also their site IDs
        values_to_update: a dictionary which will be used to update the fields of the columns

        Returns
        None
        """
        for site_column_entry in site_column_dict:
            self.update_a_site_column(
                site_id=site_column_entry["siteId"],
                column_id=site_column_entry["id"],
                values_to_update=values_to_update,
            )

    def update_a_site_column(
        self, site_id: str, column_id: str, values_to_update: Dict[Text, Any]
    ):
        """
        Updates a column with given values

        column_id: GUID of the column
        site_id: GUID of the site in which the column is present
        values_to_update: a dictionary values which will be used to update the properties of the column

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/{column_id}"
        update_an_entity(
            url=url, values_to_update=values_to_update, bearer_token=self.bearer_token
        )

    def delete_site_columns(self, column_dict: List[Dict[Text, Any]]) -> None:
        """
        Deletes columns for the given site ID and column ID

        column_dict: a dictionary values containing the column IDs which are to be deleted and
        their corresponding site IDs

        Returns
        None
        """
        for column_entry in column_dict:
            self.delete_a_site_columns(
                site_id=column_entry["siteId"], column_id=column_entry["id"]
            )

    def delete_a_site_columns(self, site_id: str, column_id: str) -> None:
        """
        Deletes a column, given its column ID and its site ID

        site_id: GUID of the site in which the column is present
        column_id: GUID of the column which is to be deleted

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/{column_id}"
        delete_an_entity(url=url, bearer_token=self.bearer_token)

    def create_site_columns(self, data: List[Dict[Text, Any]]) -> None:
        """
        Creates columns with the information provided in the data parameter
        calls create_a_site_column for each entry of column metadata dictionary

        data: parameter which contains information such as the site IDs where the columns would be created
        and their metadata information which will be used to create them

        Returns
        None
        """
        for entry in data:
            self.create_a_site_column(
                site_id=entry["siteId"],
                enforce_unique_values=entry.get("enforceUniqueValues"),
                hidden=entry.get("hidden"),
                indexed=entry.get("indexed"),
                name=entry["name"],
                text=entry.get("text"),
            )

    def create_a_site_column(
        self,
        site_id: str,
        enforce_unique_values: bool,
        hidden: bool,
        indexed: bool,
        name: str,
        text: str = None,
    ) -> None:
        """
        Creates a list with metadata information provided in the params

        site_id: GUID of the site where the column is to be created
        enforced_unique_values: if true, no two list items may have the same value for this column
        hidden: specifies whether the column is displayed in the user interface
        name: the API-facing name of the column as it appears in the fields on a listItem.
        text: details regarding the text values in the column

        Returns
        None
        """

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/columns/"
        payload = {}
        if text:
            text = ast.literal_eval(text)
            payload["text"] = text
        payload["name"] = name
        if enforce_unique_values is not None:
            payload["enforceUniqueValues"] = enforce_unique_values
        if hidden is not None:
            payload["hidden"] = hidden
        if indexed is not None:
            payload["indexed"] = indexed
        create_an_entity(url=url, payload=payload, bearer_token=self.bearer_token)

    def get_items_by_sites_and_lists(
        self, site_id: str, list_id: str, limit: int = None
    ) -> List[Dict[Text, Any]]:
        """
        Gets items' information corresponding to a site and a list

        site_id:  GUID of a site
        list_id: GUID of a list
        limit: limits the number of columns for which information is returned

        Returns
        response: metadata information/ fields corresponding to list-items of the site
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?expand=fields&select=*"
        response = get_an_entity(url=url, bearer_token=self.bearer_token)
        if limit:
            response = response[:limit]
        return response

    def get_all_items(self, limit: int = None) -> List[Dict[Text, Any]]:
        """
        Gets all the items' information associated with the account

        limit: puts a limit to the number of items returned

        Returns
        response: returns metadata information regarding all the items that are associated with that account
        """
        sites = self.get_all_sites()
        items = []
        for site in sites:
            site_id = site["id"].split(",")[1]
            for sharepoint_list in self.get_lists_by_site(site_id=site_id):
                for item_dict in self.get_items_by_sites_and_lists(
                    site_id=site["id"].split(",")[1], list_id=sharepoint_list["id"]
                ):
                    item_dict["siteName"] = site["name"]
                    item_dict["siteId"] = site["id"].split(",")[1]
                    item_dict["listId"] = sharepoint_list["id"]
                    item_dict["list_name"] = sharepoint_list["displayName"]
                    items.append(item_dict)
        if limit:
            items = items[:limit]
        return items

    def update_items(
        self, item_dict: List[Dict[Text, Text]], values_to_update: Dict[Text, Any]
    ) -> None:
        """
        Updates the given items (item_dict) with the provided values (values_to_update)
        Calls the function update_a_item for every column

        item_dict: A dictionary containing ids of the list-items which are to be updated and
        also their site IDs
        values_to_update: a dictionary which will be used to update the fields of the items

        Returns
        None
        """
        for item_entry in item_dict:
            self.update_an_item(
                site_id=item_entry["siteId"],
                list_id=item_entry["listId"],
                item_id=item_entry["id"],
                values_to_update=values_to_update,
            )

    def update_an_item(
        self,
        site_id: str,
        list_id: str,
        item_id: str,
        values_to_update: Dict[Text, Any],
    ):
        """
        Updates an item with given values

        item_id: GUID of the column
        list_id: GUID of the list
        site_id: GUID of the site in which the list is present
        values_to_update: a dictionary values which will be used to update the properties of the list-item

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
        update_an_entity(
            url=url, values_to_update=values_to_update, bearer_token=self.bearer_token
        )

    def delete_items(self, item_dict: List[Dict[Text, Any]]) -> None:
        """
        Deletes items for the given site ID and list ID

        item_dict: a dictionary values containing the item IDs which are to be deleted,
        their corresponding site IDs and their list IDs

        Returns
        None
        """
        for item_entry in item_dict:
            self.delete_an_item(
                site_id=item_entry["siteId"],
                list_id=item_entry["listId"],
                item_id=item_entry["id"],
            )

    def delete_an_item(self, site_id: str, list_id: str, item_id: str) -> None:
        """
        Deletes an item, given its item ID, its site ID and its list ID

        list_id: GUID of the list in which the site is present
        site_id: GUID of the site in which the list is present
        item_id: GUID of the item which is to be deleted

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
        delete_an_entity(url=url, bearer_token=self.bearer_token)

    def create_items(self, data: List[Dict[Text, Any]]) -> None:
        """
        Creates items with the information provided in the data parameter
        calls create_an_item for each entry of item metadata dictionary

        data: parameter which contains information such as the site IDs and list IDs where the items
        would be created and their metadata information which will be used to create them

        Returns
        None
        """
        for entry in data:
            self.create_an_item(
                site_id=entry["siteId"],
                list_id=entry["listId"],
                fields=entry.get("fields"),
            )

    def create_an_item(self, site_id: str, list_id: str, fields: str) -> None:
        """
        Creates an item with metadata information provided in the params

        site_id: GUID of the site where the list id present
        list_id: GUID of the list where the item is to be created
        fields: The values of the columns set on this list item.

        Returns
        None
        """
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/"
        payload = {}
        if fields:
            payload["fields"] = ast.literal_eval(fields)
        create_an_entity(url=url, payload=payload, bearer_token=self.bearer_token)
