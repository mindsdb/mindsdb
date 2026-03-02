
from hubspot import HubSpot
from hubspot.auth.oauth import ApiException as OAuthApiException
from hubspot.auth.oauth import OAuthApi
from hubspot.auth.oauth import TokenResponse


def oauth__connect(client_id: str, client_secret: str) -> HubSpot:
	"""
	Connect to HubSpot using OAuth credentials.

	Args:
		client_id (str): The client ID from your HubSpot app.
		client_secret (str): The client secret from your HubSpot app.

	Returns:
		HubSpot: An authenticated HubSpot client instance.

	Raises:
		ValueError: If authentication fails or credentials are invalid.
	"""
	try:
		oauth_api = OAuthApi()
		token_response: TokenResponse = oauth_api.create_token(
			grant_type="client_credentials",
			client_id=client_id,
			client_secret=client_secret,
		)
		access_token = token_response.access_token
		return HubSpot(access_token=access_token)
	except OAuthApiException as e:
		raise ValueError(f"OAuth authentication failed: {e}")