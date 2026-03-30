from collections import OrderedDict
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


connection_args = OrderedDict(
    developer_token={
        'type': ARG_TYPE.STR,
        'description': 'Microsoft Advertising API developer token',
        'label': 'Developer Token',
        'required': True,
        'secret': True,
    },
    account_id={
        'type': ARG_TYPE.STR,
        'description': 'Microsoft Advertising account ID',
        'label': 'Account ID',
        'required': True,
    },
    customer_id={
        'type': ARG_TYPE.STR,
        'description': 'Microsoft Advertising customer ID',
        'label': 'Customer ID',
        'required': True,
    },
    client_id={
        'type': ARG_TYPE.STR,
        'description': 'Azure AD application (client) ID',
        'label': 'Client ID',
        'required': True,
    },
    client_secret={
        'type': ARG_TYPE.STR,
        'description': 'Azure AD client secret',
        'label': 'Client Secret',
        'required': True,
        'secret': True,
    },
    refresh_token={
        'type': ARG_TYPE.STR,
        'description': 'OAuth refresh token (scope: https://ads.microsoft.com/msads.manage offline_access)',
        'label': 'Refresh Token',
        'required': True,
        'secret': True,
    },
    redirect_uri={
        'type': ARG_TYPE.STR,
        'description': 'OAuth redirect URI registered in Azure AD / Google Cloud Console (must match the one used during authorization)',
        'label': 'Redirect URI',
        'required': True,
    },
    auth_type={
        'type': ARG_TYPE.STR,
        'description': 'Authentication provider: "microsoft" (default) or "google" (for Microsoft Ads accounts signed in via Google)',
        'label': 'Auth Type',
        'required': False,
    },
    environment={
        'type': ARG_TYPE.STR,
        'description': 'API environment: "production" (default) or "sandbox"',
        'label': 'Environment',
        'required': False,
    },
)

connection_args_example = OrderedDict(
    developer_token='your-developer-token',
    account_id='123456789',
    customer_id='987654321',
    client_id='your-azure-client-id',
    client_secret='your-azure-client-secret',
    refresh_token='your-refresh-token',
)
