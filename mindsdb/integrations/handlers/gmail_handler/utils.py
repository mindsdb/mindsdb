import datetime as dt

import json

from google_auth_oauthlib.flow import Flow


def credentials_to_dict(credentials):
  return {'token': credentials.token,
          'refresh_token': credentials.refresh_token,
          'token_uri': credentials.token_uri,
          'client_id': credentials.client_id,
          'client_secret': credentials.client_secret,
          'scopes': credentials.scopes,
          'expiry': dt.datetime.strftime(credentials.expiry, '%Y-%m-%dT%H:%M:%S')}


class AuthException(Exception):
    def __init__(self, message, auth_url=None):
        super().__init__(message)

        self.auth_url = auth_url


def google_auth_flow(secret_file, scopes, code=None):
    # initialise flow
    flow = Flow.from_client_secrets_file(secret_file, scopes)

    # get host url from flask
    from flask import request
    flow.redirect_uri = request.headers['ORIGIN'] + '/verify-auth'

    if code:
        flow.fetch_token(code=code)
        creds = flow.credentials
        return creds
    else:
        auth_url = flow.authorization_url()[0]
        raise AuthException(f'Authorisation required. Please follow the url: {auth_url}', auth_url=auth_url)


def save_creds_to_file(creds, file_path):
    with open(file_path, 'w') as token:
        data = credentials_to_dict(creds)
        token.write(json.dumps(data))
