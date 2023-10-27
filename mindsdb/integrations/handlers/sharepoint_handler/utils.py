import json
from typing import Dict, List, Text, Any

import requests
from requests import Response


def create_an_entity(url: str, payload: Dict[Text, Any], bearer_token: str) -> None:
    """
    Makes a POST request to the given url
    Creates an entity for the given url, bearer token and payload

    url: URL to which the get request is made
    bearer_token: authentication token for the request
    payload: a dictionary which provides the metadata information regarding the entity being created

    Returns
    None
    """
    payload = json.dumps(payload, indent=2)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    getresponse(
        request_type="POST", url=url, headers=headers, payload=payload, files=[]
    )


def get_an_entity(url: str, bearer_token: str) -> Any:
    """
    Makes a GET request to the given url
    Gets the entity for the given url and bearer token

    url: URL to which the get request is made
    bearer_token: authentication token for the request

    Returns
    Dictionary or list of dictionaries containing information/metadata corresponding to the entities
    """
    payload = {}
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    response = getresponse(
        request_type="GET", url=url, headers=headers, payload=payload, files=[]
    )
    response = response.json()["value"]
    return response


def update_an_entity(
    url: str, values_to_update: Dict[Text, Any], bearer_token: str
) -> None:
    """
    Makes a PATCH request to given url with the given values_to_update and bearer_token
    updates the entity with the provided values

    url: url provided by the user
    values_to_update: values that would be used to update the entity, would be passed in the payload
    Mostly would be a dictionary mapping fields to values
    bearer_token: authentication token passed in the header to make the request

    Returns
    None
    """
    payload = values_to_update
    payload = json.dumps(payload, indent=2)
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    getresponse(
        request_type="PATCH", url=url, headers=headers, payload=payload, files=[]
    )


def delete_an_entity(url: str, bearer_token: str):
    """
    Makes a DELETE request to the given url

    url: url string provided to which the request would be made
    bearer_token: authorization token which will be used to execute the request

    Returns
    None
    """
    payload = {}
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    getresponse(
        request_type="DELETE", url=url, headers=headers, payload=payload, files=[]
    )


def bearer_token_request(tenant_id: str, client_id: str, client_secret: str) -> Any:
    """
    Sends a request to login.microsoftonline.com for the given tenant to generate a bearer token
    which is then used in making graph api call

    tenant_id: tenant ID is a globally unique identifier (GUID) for your organization
    that is different from your organization or domain name
    client_id: client ID is a globally unique identifier (GUID) for your app registered in Entra
    client_secret: client secret is the password of the service principal or the app.

    Returns
    response: Dictionary containing bearer token and other information
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": "http://localhost",
        "grant_type": "client_credentials",
        "resource": "https://graph.microsoft.com",
    }
    files = []
    headers = {}

    response = getresponse(
        request_type="POST", url=url, headers=headers, payload=payload, files=files
    )
    response = response.json()
    return response


def getresponse(
    url: str,
    payload: Dict[Text, Any],
    files: List[Any],
    headers: Dict[Text, Any],
    request_type: str,
) -> Response:
    """
    Makes a standard HTTP request based on the params provided and returns the response.
    May raise an error if the response code does not indicate success

    url: url string provided to which the request would be made
    payload: the payload which supply additional info regarding the request
    files: the files that are needed to be passed in the request
    headers: additional information regarding the request
    may also indicate the type of content passed in the payload
    request_type: the request performs different actions based on the request type
                  DELETE/POST/PATCH/GET

    Returns
    response: may return based on the response code
    """
    response = requests.request(
        request_type, url, headers=headers, data=payload, files=files
    )
    status_code = response.status_code

    if 400 <= status_code <= 499:
        raise Exception("Client error: " + response.text)

    if 500 <= status_code <= 599:
        raise Exception("Server error: " + response.text)
    return response
