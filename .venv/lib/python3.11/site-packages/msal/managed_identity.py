# Copyright (c) Microsoft Corporation.
# All rights reserved.
#
# This code is licensed under the MIT License.
import json
import logging
import os
import socket
import sys
import time
from urllib.parse import urlparse  # Python 3+
from collections import UserDict  # Python 3+
from typing import Union  # Needed in Python 3.7 & 3.8
from .token_cache import TokenCache
from .individual_cache import _IndividualCache as IndividualCache
from .throttled_http_client import ThrottledHttpClientBase, RetryAfterParser
from .cloudshell import _is_running_in_cloud_shell


logger = logging.getLogger(__name__)


class ManagedIdentityError(ValueError):
    pass


class ManagedIdentity(UserDict):
    """Feed an instance of this class to :class:`msal.ManagedIdentityClient`
    to acquire token for the specified managed identity.
    """
    # The key names used in config dict
    ID_TYPE = "ManagedIdentityIdType"  # Contains keyword ManagedIdentity so its json equivalent will be more readable
    ID = "Id"

    # Valid values for key ID_TYPE
    CLIENT_ID = "ClientId"
    RESOURCE_ID = "ResourceId"
    OBJECT_ID = "ObjectId"
    SYSTEM_ASSIGNED = "SystemAssigned"

    _types_mapping = {  # Maps type name in configuration to type name on wire
        CLIENT_ID: "client_id",
        RESOURCE_ID: "mi_res_id",
        OBJECT_ID: "object_id",
    }

    @classmethod
    def is_managed_identity(cls, unknown):
        return isinstance(unknown, ManagedIdentity) or (
            isinstance(unknown, dict) and cls.ID_TYPE in unknown)

    @classmethod
    def is_system_assigned(cls, unknown):
        return isinstance(unknown, SystemAssignedManagedIdentity) or (
            isinstance(unknown, dict)
            and unknown.get(cls.ID_TYPE) == cls.SYSTEM_ASSIGNED)

    @classmethod
    def is_user_assigned(cls, unknown):
        return isinstance(unknown, UserAssignedManagedIdentity) or (
            isinstance(unknown, dict)
            and unknown.get(cls.ID_TYPE) in cls._types_mapping
            and unknown.get(cls.ID))

    def __init__(self, identifier=None, id_type=None):
        # Undocumented. Use subclasses instead.
        super(ManagedIdentity, self).__init__({
            self.ID_TYPE: id_type,
            self.ID: identifier,
        })


class SystemAssignedManagedIdentity(ManagedIdentity):
    """Represent a system-assigned managed identity.

    It is equivalent to a Python dict of::

        {"ManagedIdentityIdType": "SystemAssigned", "Id": None}

    or a JSON blob of::

        {"ManagedIdentityIdType": "SystemAssigned", "Id": null}
    """
    def __init__(self):
        super(SystemAssignedManagedIdentity, self).__init__(id_type=self.SYSTEM_ASSIGNED)


class UserAssignedManagedIdentity(ManagedIdentity):
    """Represent a user-assigned managed identity.

    Depends on the id you provided, the outcome is equivalent to one of the below::

        {"ManagedIdentityIdType": "ClientId", "Id": "foo"}
        {"ManagedIdentityIdType": "ResourceId", "Id": "foo"}
        {"ManagedIdentityIdType": "ObjectId", "Id": "foo"}
    """
    def __init__(self, *, client_id=None, resource_id=None, object_id=None):
        if client_id and not resource_id and not object_id:
            super(UserAssignedManagedIdentity, self).__init__(
                id_type=self.CLIENT_ID, identifier=client_id)
        elif not client_id and resource_id and not object_id:
            super(UserAssignedManagedIdentity, self).__init__(
                id_type=self.RESOURCE_ID, identifier=resource_id)
        elif not client_id and not resource_id and object_id:
            super(UserAssignedManagedIdentity, self).__init__(
                id_type=self.OBJECT_ID, identifier=object_id)
        else:
            raise ManagedIdentityError(
                "You shall specify one of the three parameters: "
                "client_id, resource_id, object_id")


class _ThrottledHttpClient(ThrottledHttpClientBase):
    def __init__(self, http_client, **kwargs):
        super(_ThrottledHttpClient, self).__init__(http_client, **kwargs)
        self.get = IndividualCache(  # All MIs (except Cloud Shell) use GETs
            mapping=self._expiring_mapping,
            key_maker=lambda func, args, kwargs: "REQ {} hash={} 429/5xx/Retry-After".format(
                args[0],  # It is the endpoint, typically a constant per MI type
                self._hash(
                    # Managed Identity flavors have inconsistent parameters.
                    # We simply choose to hash them all.
                    str(kwargs.get("params")) + str(kwargs.get("data"))),
                ),
            expires_in=RetryAfterParser(5).parse,  # 5 seconds default for non-PCA
            )(http_client.get)


class ManagedIdentityClient(object):
    """This API encapsulates multiple managed identity back-ends:
    VM, App Service, Azure Automation (Runbooks), Azure Function, Service Fabric,
    and Azure Arc.

    It also provides token cache support.

    .. note::

        Cloud Shell support is NOT implemented in this class.
        Since MSAL Python 1.18 in May 2022, it has been implemented in
        :func:`PublicClientApplication.acquire_token_interactive` via calling pattern
        ``PublicClientApplication(...).acquire_token_interactive(scopes=[...], prompt="none")``.
        That is appropriate, because Cloud Shell yields a token with
        delegated permissions for the end user who has signed in to the Azure Portal
        (like what a ``PublicClientApplication`` does),
        not a token with application permissions for an app.
    """
    _instance, _tenant = socket.getfqdn(), "managed_identity"  # Placeholders

    def __init__(
        self,
        managed_identity: Union[
            dict,
            ManagedIdentity,  # Could use Type[ManagedIdentity] but it is deprecatred in Python 3.9+
            SystemAssignedManagedIdentity,
            UserAssignedManagedIdentity,
            ],
        *,
        http_client,
        token_cache=None,
        http_cache=None,
    ):
        """Create a managed identity client.

        :param managed_identity:
            It accepts an instance of :class:`SystemAssignedManagedIdentity`
            or :class:`UserAssignedManagedIdentity`.
            They are equivalent to a dict with a certain shape,
            which may be loaded from a JSON configuration file or an env var.

        :param http_client:
            An http client object. For example, you can use ``requests.Session()``,
            optionally with exponential backoff behavior demonstrated in this recipe::

                import msal, requests
                from requests.adapters import HTTPAdapter, Retry
                s = requests.Session()
                retries = Retry(total=3, backoff_factor=0.1, status_forcelist=[
                    429, 500, 501, 502, 503, 504])
                s.mount('https://', HTTPAdapter(max_retries=retries))
                managed_identity = ...
                client = msal.ManagedIdentityClient(managed_identity, http_client=s)

        :param token_cache:
            Optional. It accepts a :class:`msal.TokenCache` instance to store tokens.
            It will use an in-memory token cache by default.

        :param http_cache:
            Optional. It has the same characteristics as the
            :paramref:`msal.ClientApplication.http_cache`.

        Recipe 1: Hard code a managed identity for your app::

            import msal, requests
            client = msal.ManagedIdentityClient(
                msal.UserAssignedManagedIdentity(client_id="foo"),
                http_client=requests.Session(),
                )
            token = client.acquire_token_for_client("resource")

        Recipe 2: Write once, run everywhere.
        If you use different managed identity on different deployment,
        you may use an environment variable (such as MY_MANAGED_IDENTITY_CONFIG)
        to store a json blob like
        ``{"ManagedIdentityIdType": "ClientId", "Id": "foo"}`` or
        ``{"ManagedIdentityIdType": "SystemAssignedManagedIdentity", "Id": null})``.
        The following app can load managed identity configuration dynamically::

            import json, os, msal, requests
            config = os.getenv("MY_MANAGED_IDENTITY_CONFIG")
            assert config, "An ENV VAR with value should exist"
            client = msal.ManagedIdentityClient(
                json.loads(config),
                http_client=requests.Session(),
                )
            token = client.acquire_token_for_client("resource")
        """
        self._managed_identity = managed_identity
        self._http_client = _ThrottledHttpClient(
            # This class only throttles excess token acquisition requests.
            # It does not provide retry.
            # Retry is the http_client or caller's responsibility, not MSAL's.
            #
            # FWIW, here is the inconsistent retry recommendation.
            # 1. Only MI on VM defines exotic 404 and 410 retry recommendations
            #    ( https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/how-to-use-vm-token#error-handling )
            #    (especially for 410 which was supposed to be a permanent failure).
            # 2. MI on Service Fabric specifically suggests to not retry on 404.
            #    ( https://learn.microsoft.com/en-us/azure/service-fabric/how-to-managed-cluster-managed-identity-service-fabric-app-code#error-handling )
            http_client.http_client  # Patch the raw (unpatched) http client
                if isinstance(http_client, ThrottledHttpClientBase) else http_client,
            http_cache=http_cache,
        )
        self._token_cache = token_cache or TokenCache()

    def acquire_token_for_client(self, *, resource):  # We may support scope in the future
        """Acquire token for the managed identity.

        The result will be automatically cached.
        Subsequent calls will automatically search from cache first.

        .. note::

            Known issue: When an Azure VM has only one user-assigned managed identity,
            and your app specifies to use system-assigned managed identity,
            Azure VM may still return a token for your user-assigned identity.

            This is a service-side behavior that cannot be changed by this library.
            `Azure VM docs <https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>`_
        """
        access_token_from_cache = None
        client_id_in_cache = self._managed_identity.get(
            ManagedIdentity.ID, "SYSTEM_ASSIGNED_MANAGED_IDENTITY")
        if True:  # Does not offer an "if not force_refresh" option, because
                  # there would be built-in token cache in the service side anyway
            matches = self._token_cache.find(
                self._token_cache.CredentialType.ACCESS_TOKEN,
                target=[resource],
                query=dict(
                    client_id=client_id_in_cache,
                    environment=self._instance,
                    realm=self._tenant,
                    home_account_id=None,
                ),
            )
            now = time.time()
            for entry in matches:
                expires_in = int(entry["expires_on"]) - now
                if expires_in < 5*60:  # Then consider it expired
                    continue  # Removal is not necessary, it will be overwritten
                logger.debug("Cache hit an AT")
                access_token_from_cache = {  # Mimic a real response
                    "access_token": entry["secret"],
                    "token_type": entry.get("token_type", "Bearer"),
                    "expires_in": int(expires_in),  # OAuth2 specs defines it as int
                }
                if "refresh_on" in entry and int(entry["refresh_on"]) < now:  # aging
                    break  # With a fallback in hand, we break here to go refresh
                return access_token_from_cache  # It is still good as new
        try:
            result = _obtain_token(self._http_client, self._managed_identity, resource)
            if "access_token" in result:
                expires_in = result.get("expires_in", 3600)
                if "refresh_in" not in result and expires_in >= 7200:
                    result["refresh_in"] = int(expires_in / 2)
                self._token_cache.add(dict(
                    client_id=client_id_in_cache,
                    scope=[resource],
                    token_endpoint="https://{}/{}".format(self._instance, self._tenant),
                    response=result,
                    params={},
                    data={},
                ))
            if (result and "error" not in result) or (not access_token_from_cache):
                return result
        except:  # The exact HTTP exception is transportation-layer dependent
            # Typically network error. Potential AAD outage?
            if not access_token_from_cache:  # It means there is no fall back option
                raise  # We choose to bubble up the exception
        return access_token_from_cache


def _scope_to_resource(scope):  # This is an experimental reasonable-effort approach
    u = urlparse(scope)
    if u.scheme:
        return "{}://{}".format(u.scheme, u.netloc)
    return scope  # There is no much else we can do here


APP_SERVICE = object()
AZURE_ARC = object()
CLOUD_SHELL = object()  # In MSAL Python, token acquisition was done by
    # PublicClientApplication(...).acquire_token_interactive(..., prompt="none")
MACHINE_LEARNING = object()
SERVICE_FABRIC = object()
DEFAULT_TO_VM = object()  # Unknown environment; default to VM; you may want to probe
def get_managed_identity_source():
    """Detect the current environment and return the likely identity source.

    When this function returns ``CLOUD_SHELL``, you should use
    :func:`msal.PublicClientApplication.acquire_token_interactive` with ``prompt="none"``
    to obtain a token.
    """
    if ("IDENTITY_ENDPOINT" in os.environ and "IDENTITY_HEADER" in os.environ
            and "IDENTITY_SERVER_THUMBPRINT" in os.environ
    ):
        return SERVICE_FABRIC
    if "IDENTITY_ENDPOINT" in os.environ and "IDENTITY_HEADER" in os.environ:
        return APP_SERVICE
    if "MSI_ENDPOINT" in os.environ and "MSI_SECRET" in os.environ:
        return MACHINE_LEARNING
    if "IDENTITY_ENDPOINT" in os.environ and "IMDS_ENDPOINT" in os.environ:
        return AZURE_ARC
    if _is_running_in_cloud_shell():
        return CLOUD_SHELL
    return DEFAULT_TO_VM


def _obtain_token(http_client, managed_identity, resource):
    # A unified low-level API that talks to different Managed Identity
    if ("IDENTITY_ENDPOINT" in os.environ and "IDENTITY_HEADER" in os.environ
            and "IDENTITY_SERVER_THUMBPRINT" in os.environ
    ):
        if managed_identity:
            logger.debug(
                "Ignoring managed_identity parameter. "
                "Managed Identity in Service Fabric is configured in the cluster, "
                "not during runtime. See also "
                "https://learn.microsoft.com/en-us/azure/service-fabric/configure-existing-cluster-enable-managed-identity-token-service")
        return _obtain_token_on_service_fabric(
            http_client,
            os.environ["IDENTITY_ENDPOINT"],
            os.environ["IDENTITY_HEADER"],
            os.environ["IDENTITY_SERVER_THUMBPRINT"],
            resource,
        )
    if "IDENTITY_ENDPOINT" in os.environ and "IDENTITY_HEADER" in os.environ:
        return _obtain_token_on_app_service(
            http_client,
            os.environ["IDENTITY_ENDPOINT"],
            os.environ["IDENTITY_HEADER"],
            managed_identity,
            resource,
        )
    if "MSI_ENDPOINT" in os.environ and "MSI_SECRET" in os.environ:
        # Back ported from https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.15.0/sdk/identity/azure-identity/azure/identity/_credentials/azure_ml.py
        return _obtain_token_on_machine_learning(
            http_client,
            os.environ["MSI_ENDPOINT"],
            os.environ["MSI_SECRET"],
            managed_identity,
            resource,
        )
    if "IDENTITY_ENDPOINT" in os.environ and "IMDS_ENDPOINT" in os.environ:
        if ManagedIdentity.is_user_assigned(managed_identity):
            raise ManagedIdentityError(  # Note: Azure Identity for Python raised exception too
                "Invalid managed_identity parameter. "
                "Azure Arc supports only system-assigned managed identity, "
                "See also "
                "https://learn.microsoft.com/en-us/azure/service-fabric/configure-existing-cluster-enable-managed-identity-token-service")
        return _obtain_token_on_arc(
            http_client,
            os.environ["IDENTITY_ENDPOINT"],
            resource,
        )
    return _obtain_token_on_azure_vm(http_client, managed_identity, resource)


def _adjust_param(params, managed_identity):
    # Modify the params dict in place
    id_name = ManagedIdentity._types_mapping.get(
        managed_identity.get(ManagedIdentity.ID_TYPE))
    if id_name:
        params[id_name] = managed_identity[ManagedIdentity.ID]

def _obtain_token_on_azure_vm(http_client, managed_identity, resource):
    # Based on https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
    logger.debug("Obtaining token via managed identity on Azure VM")
    params = {
        "api-version": "2018-02-01",
        "resource": resource,
        }
    _adjust_param(params, managed_identity)
    resp = http_client.get(
        "http://169.254.169.254/metadata/identity/oauth2/token",
        params=params,
        headers={"Metadata": "true"},
        )
    try:
        payload = json.loads(resp.text)
        if payload.get("access_token") and payload.get("expires_in"):
            return {  # Normalizing the payload into OAuth2 format
                "access_token": payload["access_token"],
                "expires_in": int(payload["expires_in"]),
                "resource": payload.get("resource"),
                "token_type": payload.get("token_type", "Bearer"),
                }
        return payload  # Typically an error, but it is undefined in the doc above
    except json.decoder.JSONDecodeError:
        logger.debug("IMDS emits unexpected payload: %s", resp.text)
        raise

def _obtain_token_on_app_service(
    http_client, endpoint, identity_header, managed_identity, resource,
):
    """Obtains token for
    `App Service <https://learn.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=portal%2Chttp#rest-endpoint-reference>`_,
    Azure Functions, and Azure Automation.
    """
    # Prerequisite: Create your app service https://docs.microsoft.com/en-us/azure/app-service/quickstart-python
    # Assign it a managed identity https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=portal%2Chttp
    # SSH into your container for testing https://docs.microsoft.com/en-us/azure/app-service/configure-linux-open-ssh-session
    logger.debug("Obtaining token via managed identity on Azure App Service")
    params = {
        "api-version": "2019-08-01",
        "resource": resource,
        }
    _adjust_param(params, managed_identity)
    resp = http_client.get(
        endpoint,
        params=params,
        headers={
            "X-IDENTITY-HEADER": identity_header,
            "Metadata": "true",  # Unnecessary yet harmless for App Service,
            # It will be needed by Azure Automation
            # https://docs.microsoft.com/en-us/azure/automation/enable-managed-identity-for-automation#get-access-token-for-system-assigned-managed-identity-using-http-get
            },
        )
    try:
        payload = json.loads(resp.text)
        if payload.get("access_token") and payload.get("expires_on"):
            return {  # Normalizing the payload into OAuth2 format
                "access_token": payload["access_token"],
                "expires_in": int(payload["expires_on"]) - int(time.time()),
                "resource": payload.get("resource"),
                "token_type": payload.get("token_type", "Bearer"),
                }
        return {
            "error": "invalid_scope",  # Empirically, wrong resource ends up with a vague statusCode=500
            "error_description": "{}, {}".format(
                payload.get("statusCode"), payload.get("message")),
            }
    except json.decoder.JSONDecodeError:
        logger.debug("IMDS emits unexpected payload: %s", resp.text)
        raise

def _obtain_token_on_machine_learning(
    http_client, endpoint, secret, managed_identity, resource,
):
    # Could not find protocol docs from https://docs.microsoft.com/en-us/azure/machine-learning
    # The following implementation is back ported from Azure Identity 1.15.0
    logger.debug("Obtaining token via managed identity on Azure Machine Learning")
    params = {"api-version": "2017-09-01", "resource": resource}
    _adjust_param(params, managed_identity)
    if params["api-version"] == "2017-09-01" and "client_id" in params:
        # Workaround for a known bug in Azure ML 2017 API
        params["clientid"] = params.pop("client_id")
    resp = http_client.get(
        endpoint,
        params=params,
        headers={"secret": secret},
        )
    try:
        payload = json.loads(resp.text)
        if payload.get("access_token") and payload.get("expires_on"):
            return {  # Normalizing the payload into OAuth2 format
                "access_token": payload["access_token"],
                "expires_in": int(payload["expires_on"]) - int(time.time()),
                "resource": payload.get("resource"),
                "token_type": payload.get("token_type", "Bearer"),
                }
        return {
            "error": "invalid_scope",  # TODO: To be tested
            "error_description": "{}".format(payload),
            }
    except json.decoder.JSONDecodeError:
        logger.debug("IMDS emits unexpected payload: %s", resp.text)
        raise


def _obtain_token_on_service_fabric(
    http_client, endpoint, identity_header, server_thumbprint, resource,
):
    """Obtains token for
    `Service Fabric <https://learn.microsoft.com/en-us/azure/service-fabric/>`_
    """
    # Deployment https://learn.microsoft.com/en-us/azure/service-fabric/service-fabric-get-started-containers-linux
    # See also https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/tests/managed-identity-live/service-fabric/service_fabric.md
    # Protocol https://learn.microsoft.com/en-us/azure/service-fabric/how-to-managed-identity-service-fabric-app-code#acquiring-an-access-token-using-rest-api
    logger.debug("Obtaining token via managed identity on Azure Service Fabric")
    resp = http_client.get(
        endpoint,
        params={"api-version": "2019-07-01-preview", "resource": resource},
        headers={"Secret": identity_header},
        )
    try:
        payload = json.loads(resp.text)
        if payload.get("access_token") and payload.get("expires_on"):
            return {  # Normalizing the payload into OAuth2 format
                "access_token": payload["access_token"],
                "expires_in": int(  # Despite the example in docs shows an integer,
                    payload["expires_on"]  # Azure SDK team's test obtained a string.
                    ) - int(time.time()),
                "resource": payload.get("resource"),
                "token_type": payload["token_type"],
                }
        error = payload.get("error", {})  # https://learn.microsoft.com/en-us/azure/service-fabric/how-to-managed-identity-service-fabric-app-code#error-handling
        error_mapping = {  # Map Service Fabric errors into OAuth2 errors  https://www.rfc-editor.org/rfc/rfc6749#section-5.2
            "SecretHeaderNotFound": "unauthorized_client",
            "ManagedIdentityNotFound": "invalid_client",
            "ArgumentNullOrEmpty": "invalid_scope",
            }
        return {
            "error": error_mapping.get(payload["error"]["code"], "invalid_request"),
            "error_description": resp.text,
            }
    except json.decoder.JSONDecodeError:
        logger.debug("IMDS emits unexpected payload: %s", resp.text)
        raise


_supported_arc_platforms_and_their_prefixes = {
    "linux": "/var/opt/azcmagent/tokens",
    "win32": os.path.expandvars(r"%ProgramData%\AzureConnectedMachineAgent\Tokens"),
}

class ArcPlatformNotSupportedError(ManagedIdentityError):
    pass

def _obtain_token_on_arc(http_client, endpoint, resource):
    # https://learn.microsoft.com/en-us/azure/azure-arc/servers/managed-identity-authentication
    logger.debug("Obtaining token via managed identity on Azure Arc")
    resp = http_client.get(
        endpoint,
        params={"api-version": "2020-06-01", "resource": resource},
        headers={"Metadata": "true"},
        )
    www_auth = "www-authenticate"  # Header in lower case
    challenge = {
        # Normalized to lowercase, because header names are case-insensitive
        # https://datatracker.ietf.org/doc/html/rfc7230#section-3.2
        k.lower(): v for k, v in resp.headers.items() if k.lower() == www_auth
        }.get(www_auth, "").split("=")  # Output will be ["Basic realm", "content"]
    if not (  # https://datatracker.ietf.org/doc/html/rfc7617#section-2
            len(challenge) == 2 and challenge[0].lower() == "basic realm"):
        raise ManagedIdentityError(
            "Unrecognizable WWW-Authenticate header: {}".format(resp.headers))
    if sys.platform not in _supported_arc_platforms_and_their_prefixes:
        raise ArcPlatformNotSupportedError(
            f"Platform {sys.platform} was undefined and unsupported")
    filename = os.path.join(
        # This algorithm is documented in an internal doc https://msazure.visualstudio.com/One/_wiki/wikis/One.wiki/233012/VM-Extension-Authoring-for-Arc?anchor=2.-obtaining-tokens
        _supported_arc_platforms_and_their_prefixes[sys.platform],
        os.path.splitext(os.path.basename(challenge[1]))[0] + ".key")
    if os.stat(filename).st_size > 4096:  # Check size BEFORE loading its content
        raise ManagedIdentityError("Local key file shall not be larger than 4KB")
    with open(filename) as f:
        secret = f.read()
    response = http_client.get(
        endpoint,
        params={"api-version": "2020-06-01", "resource": resource},
        headers={"Metadata": "true", "Authorization": "Basic {}".format(secret)},
        )
    try:
        payload = json.loads(response.text)
        if payload.get("access_token") and payload.get("expires_in"):
            # Example: https://learn.microsoft.com/en-us/azure/azure-arc/servers/media/managed-identity-authentication/bash-token-output-example.png
            return {
                "access_token": payload["access_token"],
                "expires_in": int(payload["expires_in"]),
                "token_type": payload.get("token_type", "Bearer"),
                "resource": payload.get("resource"),
                }
    except json.decoder.JSONDecodeError:
        pass
    return {
        "error": "invalid_request",
        "error_description": response.text,
        }

