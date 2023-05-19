import requests
from typing import Dict, List


class FrappeClient(object):
    """Client to interact with the Frappe API.
    
    Attributes:
        domain (str): Path to Frappe domain to use (e.g. https://mindsdbfrappe.com).
        access_token (str): Frappe authorization token to use for all API requests.
    """

    def __init__(
            self,
            domain: str,
            access_token: str):
        self.domain = domain
        self.base_url = f'{self.domain}/api'
        self.access_token = access_token

        self.headers = {
            'Authorization': f'token {self.access_token}',
        }

    def get_document(self, doctype: str, name: str) -> Dict:
        """Gets a document matching the given doctype from Frappe.
        
        See https://frappeframework.com/docs/v14/user/en/api/rest#listing-documents
        Args:
            doctype (str): The document type to retrieve.
            name (str): Name of the document.
        """
        document_response = requests.get(
            f'{self.base_url}/resource/{doctype}/{name}',
            headers=self.headers)
        if not document_response.ok:
            document_response.raise_for_status()
        return document_response.json()['data']

    def get_documents(self, doctype: str, limit: int = None, filters: List[List] = None) -> List[Dict]:
        """Gets all documents matching the given doctype from Frappe.
        
        See https://frappeframework.com/docs/v14/user/en/api/rest#listing-documents
        Args:
            doctype (str): The document type to retrieve.
            limit (int): At most, how many messages to return.
            filters (List[List]): List of filters in the form [field, operator, value] e.g. ["amount", ">", 50]
        """
        params = {}
        if limit is not None:
            params['limit'] = limit
        if filters is not None:
            params['filters'] = filters
        documents_response = requests.get(
            f'{self.base_url}/resource/{doctype}',
            params=params,
            headers=self.headers)
        if not documents_response.ok:
            documents_response.raise_for_status()
        return documents_response.json()['data']

    def post_document(
            self,
            doctype: str,
            data: Dict):
        """Creates a new document of the given doctype.
        See https://frappeframework.com/docs/v14/user/en/api/rest#listing-documents
        
        Args:
            doctype (str): Type of the document to create.
            data (Dict): Document object.
        """
        post_response = requests.post(
            f'{self.base_url}/resource/{doctype}',
            json=data,
            headers=self.headers)
        if not post_response.ok:
            post_response.raise_for_status()
        return post_response.json()['data']

    def ping(self) -> bool:
        """Sends a basic request to the Frappe API to see if it succeeds.
        
        Returns whether or not the connection to the Frappe API is valid.
        See https://frappeframework.com/docs/v14/user/en/api/rest#1-token-based-authentication
        """

        # No ping or similar endpoint exists, so we'll try getting the logged in user.
        user_response = requests.get(
            f'{self.base_url}/method/frappe.auth.get_logged_user',
            headers=self.headers)
        return user_response.ok
