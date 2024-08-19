"""The LangSmith Client."""

from __future__ import annotations

import atexit
import collections
import datetime
import functools
import importlib
import importlib.metadata
import io
import json
import logging
import os
import random
import re
import socket
import sys
import threading
import time
import typing
import uuid
import warnings
import weakref
from dataclasses import dataclass, field
from queue import Empty, PriorityQueue, Queue
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)
from urllib import parse as urllib_parse

import orjson
import requests
from requests import adapters as requests_adapters
from urllib3.util import Retry

import langsmith
from langsmith import env as ls_env
from langsmith import schemas as ls_schemas
from langsmith import utils as ls_utils

if TYPE_CHECKING:
    import pandas as pd  # type: ignore

    from langsmith.evaluation import evaluator as ls_evaluator

logger = logging.getLogger(__name__)
_urllib3_logger = logging.getLogger("urllib3.connectionpool")

X_API_KEY = "x-api-key"


def _is_localhost(url: str) -> bool:
    """Check if the URL is localhost.

    Parameters
    ----------
    url : str
        The URL to check.

    Returns:
    -------
    bool
        True if the URL is localhost, False otherwise.
    """
    try:
        netloc = urllib_parse.urlsplit(url).netloc.split(":")[0]
        ip = socket.gethostbyname(netloc)
        return ip == "127.0.0.1" or ip.startswith("0.0.0.0") or ip.startswith("::")
    except socket.gaierror:
        return False


def _parse_token_or_url(
    url_or_token: Union[str, uuid.UUID], api_url: str, num_parts: int = 2
) -> Tuple[str, str]:
    """Parse a public dataset URL or share token."""
    try:
        if isinstance(url_or_token, uuid.UUID) or uuid.UUID(url_or_token):
            return api_url, str(url_or_token)
    except ValueError:
        pass

    # Then it's a URL
    parsed_url = urllib_parse.urlparse(str(url_or_token))
    # Extract the UUID from the path
    path_parts = parsed_url.path.split("/")
    if len(path_parts) >= num_parts:
        token_uuid = path_parts[-num_parts]
    else:
        raise ls_utils.LangSmithUserError(f"Invalid public dataset URL: {url_or_token}")
    return api_url, token_uuid


def _is_langchain_hosted(url: str) -> bool:
    """Check if the URL is langchain hosted.

    Parameters
    ----------
    url : str
        The URL to check.

    Returns:
    -------
    bool
        True if the URL is langchain hosted, False otherwise.
    """
    try:
        netloc = urllib_parse.urlsplit(url).netloc.split(":")[0]
        return netloc.endswith("langchain.com")
    except Exception:
        return False


ID_TYPE = Union[uuid.UUID, str]
RUN_TYPE_T = Literal[
    "tool", "chain", "llm", "retriever", "embedding", "prompt", "parser"
]


def _default_retry_config() -> Retry:
    """Get the default retry configuration.

    If urllib3 version is 1.26 or greater, retry on all methods.

    Returns:
    -------
    Retry
        The default retry configuration.
    """
    retry_params = dict(
        total=3,
        status_forcelist=[502, 503, 504, 408, 425],
        backoff_factor=0.5,
        # Sadly urllib3 1.x doesn't support backoff_jitter
        raise_on_redirect=False,
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    # the `allowed_methods` keyword is not available in urllib3 < 1.26

    # check to see if urllib3 version is 1.26 or greater
    urllib3_version = importlib.metadata.version("urllib3")
    use_allowed_methods = tuple(map(int, urllib3_version.split("."))) >= (1, 26)

    if use_allowed_methods:
        # Retry on all methods
        retry_params["allowed_methods"] = None

    return ls_utils.LangSmithRetry(**retry_params)  # type: ignore


_MAX_DEPTH = 2


def _simple_default(obj: Any) -> Any:
    # Don't traverse into nested objects
    try:
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.loads(json.dumps(obj))
    except BaseException as e:
        logger.debug(f"Failed to serialize {type(obj)} to JSON: {e}")
        return repr(obj)


def _serialize_json(obj: Any, depth: int = 0, serialize_py: bool = True) -> Any:
    try:
        if depth >= _MAX_DEPTH:
            try:
                return orjson.loads(_dumps_json_single(obj))
            except BaseException:
                return repr(obj)
        if isinstance(obj, bytes):
            return obj.decode("utf-8")
        if isinstance(obj, (set, tuple)):
            return orjson.loads(_dumps_json_single(list(obj)))

        serialization_methods = [
            ("model_dump_json", True),  # Pydantic V2
            ("json", True),  # Pydantic V1
            ("to_json", False),  # dataclass_json
            ("model_dump", True),  # Pydantic V2 with non-serializable fields
            ("dict", False),  # Pydantic V1 with non-serializable fields
        ]
        for attr, exclude_none in serialization_methods:
            if hasattr(obj, attr) and callable(getattr(obj, attr)):
                try:
                    method = getattr(obj, attr)
                    json_str = (
                        method(exclude_none=exclude_none) if exclude_none else method()
                    )
                    if isinstance(json_str, str):
                        return json.loads(json_str)
                    return orjson.loads(
                        _dumps_json(
                            json_str, depth=depth + 1, serialize_py=serialize_py
                        )
                    )
                except Exception as e:
                    logger.debug(f"Failed to serialize {type(obj)} to JSON: {e}")
                    pass
        if serialize_py:
            all_attrs = {}
            if hasattr(obj, "__slots__"):
                all_attrs.update(
                    {slot: getattr(obj, slot, None) for slot in obj.__slots__}
                )
            if hasattr(obj, "__dict__"):
                all_attrs.update(vars(obj))
            if all_attrs:
                filtered = {
                    k: v if v is not obj else repr(v) for k, v in all_attrs.items()
                }
                return orjson.loads(
                    _dumps_json(filtered, depth=depth + 1, serialize_py=serialize_py)
                )
        return repr(obj)
    except BaseException as e:
        logger.debug(f"Failed to serialize {type(obj)} to JSON: {e}")
        return repr(obj)


def _elide_surrogates(s: bytes) -> bytes:
    pattern = re.compile(rb"\\ud[89a-f][0-9a-f]{2}", re.IGNORECASE)
    result = pattern.sub(b"", s)
    return result


def _dumps_json_single(
    obj: Any, default: Optional[Callable[[Any], Any]] = None
) -> bytes:
    try:
        return orjson.dumps(
            obj,
            default=default,
            option=orjson.OPT_SERIALIZE_NUMPY
            | orjson.OPT_SERIALIZE_DATACLASS
            | orjson.OPT_SERIALIZE_UUID
            | orjson.OPT_NON_STR_KEYS,
        )
    except TypeError as e:
        # Usually caused by UTF surrogate characters
        logger.debug(f"Orjson serialization failed: {repr(e)}. Falling back to json.")
        result = json.dumps(
            obj,
            default=_simple_default,
            ensure_ascii=True,
        ).encode("utf-8")
        try:
            result = orjson.dumps(orjson.loads(result.decode("utf-8", errors="lossy")))
        except orjson.JSONDecodeError:
            result = _elide_surrogates(result)
        return result


def _dumps_json(obj: Any, depth: int = 0, serialize_py: bool = True) -> bytes:
    """Serialize an object to a JSON formatted string.

    Parameters
    ----------
    obj : Any
        The object to serialize.
    default : Callable[[Any], Any] or None, default=None
        The default function to use for serialization.

    Returns:
    -------
    str
        The JSON formatted string.
    """
    return _dumps_json_single(
        obj, functools.partial(_serialize_json, depth=depth, serialize_py=serialize_py)
    )


def close_session(session: requests.Session) -> None:
    """Close the session.

    Parameters
    ----------
    session : Session
        The session to close.
    """
    logger.debug("Closing Client.session")
    session.close()


def _validate_api_key_if_hosted(api_url: str, api_key: Optional[str]) -> None:
    """Verify API key is provided if url not localhost.

    Parameters
    ----------
    api_url : str
        The API URL.
    api_key : str or None
        The API key.

    Raises:
    ------
    LangSmithUserError
        If the API key is not provided when using the hosted service.
    """
    # If the domain is langchain.com, raise error if no api_key
    if not api_key:
        if _is_langchain_hosted(api_url):
            raise ls_utils.LangSmithUserError(
                "API key must be provided when using hosted LangSmith API"
            )


def _get_tracing_sampling_rate() -> float | None:
    """Get the tracing sampling rate.

    Returns:
    -------
    float
        The tracing sampling rate.
    """
    sampling_rate_str = ls_utils.get_env_var("TRACING_SAMPLING_RATE")
    if sampling_rate_str is None:
        return None
    sampling_rate = float(sampling_rate_str)
    if sampling_rate < 0 or sampling_rate > 1:
        raise ls_utils.LangSmithUserError(
            "LANGSMITH_TRACING_SAMPLING_RATE must be between 0 and 1 if set."
            f" Got: {sampling_rate}"
        )
    return sampling_rate


def _get_env(var_names: Sequence[str], default: Optional[str] = None) -> Optional[str]:
    for var_name in var_names:
        var = os.getenv(var_name)
        if var is not None:
            return var
    return default


def _get_api_key(api_key: Optional[str]) -> Optional[str]:
    api_key_ = (
        api_key
        if api_key is not None
        else _get_env(("LANGSMITH_API_KEY", "LANGCHAIN_API_KEY"))
    )
    if api_key_ is None or not api_key_.strip():
        return None
    return api_key_.strip().strip('"').strip("'")


def _get_api_url(api_url: Optional[str]) -> str:
    _api_url = api_url or cast(
        str,
        _get_env(
            ("LANGSMITH_ENDPOINT", "LANGCHAIN_ENDPOINT"),
            "https://api.smith.langchain.com",
        ),
    )
    if not _api_url.strip():
        raise ls_utils.LangSmithUserError("LangSmith API URL cannot be empty")
    return _api_url.strip().strip('"').strip("'").rstrip("/")


def _get_write_api_urls(_write_api_urls: Optional[Dict[str, str]]) -> Dict[str, str]:
    _write_api_urls = _write_api_urls or json.loads(
        os.getenv("LANGSMITH_RUNS_ENDPOINTS", "{}")
    )
    processed_write_api_urls = {}
    for url, api_key in _write_api_urls.items():
        processed_url = url.strip()
        if not processed_url:
            raise ls_utils.LangSmithUserError(
                "LangSmith runs API URL within LANGSMITH_RUNS_ENDPOINTS cannot be empty"
            )
        processed_url = processed_url.strip().strip('"').strip("'").rstrip("/")
        processed_api_key = api_key.strip().strip('"').strip("'")
        _validate_api_key_if_hosted(processed_url, processed_api_key)
        processed_write_api_urls[processed_url] = processed_api_key

    return processed_write_api_urls


def _as_uuid(value: ID_TYPE, var: Optional[str] = None) -> uuid.UUID:
    try:
        return uuid.UUID(value) if not isinstance(value, uuid.UUID) else value
    except ValueError as e:
        var = var or "value"
        raise ls_utils.LangSmithUserError(
            f"{var} must be a valid UUID or UUID string. Got {value}"
        ) from e


@typing.overload
def _ensure_uuid(value: Optional[Union[str, uuid.UUID]]) -> uuid.UUID: ...


@typing.overload
def _ensure_uuid(
    value: Optional[Union[str, uuid.UUID]], *, accept_null: bool = True
) -> Optional[uuid.UUID]: ...


def _ensure_uuid(value: Optional[Union[str, uuid.UUID]], *, accept_null: bool = False):
    if value is None:
        if accept_null:
            return None
        return uuid.uuid4()
    return _as_uuid(value)


@functools.lru_cache(maxsize=1)
def _parse_url(url):
    parsed_url = urllib_parse.urlparse(url)
    host = parsed_url.netloc.split(":")[0]
    return host


@dataclass(order=True)
class TracingQueueItem:
    """An item in the tracing queue.

    Attributes:
        priority (str): The priority of the item.
        action (str): The action associated with the item.
        item (Any): The item itself.
    """

    priority: str
    action: str
    item: Any = field(compare=False)


class Client:
    """Client for interacting with the LangSmith API."""

    __slots__ = [
        "__weakref__",
        "api_url",
        "api_key",
        "retry_config",
        "timeout_ms",
        "session",
        "_get_data_type_cached",
        "_web_url",
        "_tenant_id",
        "tracing_sample_rate",
        "_sampled_post_uuids",
        "tracing_queue",
        "_anonymizer",
        "_hide_inputs",
        "_hide_outputs",
        "_info",
        "_write_api_urls",
    ]

    def __init__(
        self,
        api_url: Optional[str] = None,
        *,
        api_key: Optional[str] = None,
        retry_config: Optional[Retry] = None,
        timeout_ms: Optional[Union[int, Tuple[int, int]]] = None,
        web_url: Optional[str] = None,
        session: Optional[requests.Session] = None,
        auto_batch_tracing: bool = True,
        anonymizer: Optional[Callable[[dict], dict]] = None,
        hide_inputs: Optional[Union[Callable[[dict], dict], bool]] = None,
        hide_outputs: Optional[Union[Callable[[dict], dict], bool]] = None,
        info: Optional[Union[dict, ls_schemas.LangSmithInfo]] = None,
        api_urls: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize a Client instance.

        Parameters
        ----------
        api_url : str or None, default=None
            URL for the LangSmith API. Defaults to the LANGCHAIN_ENDPOINT
            environment variable or https://api.smith.langchain.com if not set.
        api_key : str or None, default=None
            API key for the LangSmith API. Defaults to the LANGCHAIN_API_KEY
            environment variable.
        retry_config : Retry or None, default=None
            Retry configuration for the HTTPAdapter.
        timeout_ms : int or None, default=None
            Timeout in milliseconds for the HTTPAdapter.
        web_url : str or None, default=None
            URL for the LangSmith web app. Default is auto-inferred from
            the ENDPOINT.
        session: requests.Session or None, default=None
            The session to use for requests. If None, a new session will be
            created.
        anonymizer : Optional[Callable[[dict], dict]]
            A function applied for masking serialized run inputs and outputs,
            before sending to the API.
        hide_inputs: Whether to hide run inputs when tracing with this client.
            If True, hides the entire inputs. If a function, applied to
            all run inputs when creating runs.
        hide_outputs: Whether to hide run outputs when tracing with this client.
            If True, hides the entire outputs. If a function, applied to
            all run outputs when creating runs.
        info: Optional[ls_schemas.LangSmithInfo]
            The information about the LangSmith API. If not provided, it will
            be fetched from the API.
        api_urls: Optional[Dict[str, str]]
            A dictionary of write API URLs and their corresponding API keys.
            Useful for multi-tenant setups. Data is only read from the first
            URL in the dictionary. However, ONLY Runs are written (POST and PATCH)
            to all URLs in the dictionary. Feedback, sessions, datasets, examples,
            annotation queues and evaluation results are only written to the first.

        Raises:
        ------
        LangSmithUserError
            If the API key is not provided when using the hosted service.
            If both api_url and api_urls are provided.
        """
        if api_url and api_urls:
            raise ls_utils.LangSmithUserError(
                "You cannot provide both api_url and api_urls."
            )

        if (
            os.getenv("LANGSMITH_ENDPOINT") or os.getenv("LANGCHAIN_ENDPOINT")
        ) and os.getenv("LANGSMITH_RUNS_ENDPOINTS"):
            raise ls_utils.LangSmithUserError(
                "You cannot provide both LANGSMITH_ENDPOINT / LANGCHAIN_ENDPOINT "
                "and LANGSMITH_RUNS_ENDPOINTS."
            )

        self.tracing_sample_rate = _get_tracing_sampling_rate()
        self._sampled_post_uuids: set[uuid.UUID] = set()
        self._write_api_urls: Mapping[str, Optional[str]] = _get_write_api_urls(
            api_urls
        )
        if self._write_api_urls:
            self.api_url = next(iter(self._write_api_urls))
            self.api_key: Optional[str] = self._write_api_urls[self.api_url]
        else:
            self.api_url = _get_api_url(api_url)
            self.api_key = _get_api_key(api_key)
            _validate_api_key_if_hosted(self.api_url, self.api_key)
            self._write_api_urls = {self.api_url: self.api_key}
        self.retry_config = retry_config or _default_retry_config()
        self.timeout_ms = (
            (timeout_ms, timeout_ms)
            if isinstance(timeout_ms, int)
            else (timeout_ms or (10_000, 90_001))
        )
        self._web_url = web_url
        self._tenant_id: Optional[uuid.UUID] = None
        # Create a session and register a finalizer to close it
        session_ = session if session else requests.Session()
        self.session = session_
        self._info = (
            info
            if info is None or isinstance(info, ls_schemas.LangSmithInfo)
            else ls_schemas.LangSmithInfo(**info)
        )
        weakref.finalize(self, close_session, self.session)
        atexit.register(close_session, session_)
        # Initialize auto batching
        if auto_batch_tracing:
            self.tracing_queue: Optional[PriorityQueue] = PriorityQueue()

            threading.Thread(
                target=_tracing_control_thread_func,
                # arg must be a weakref to self to avoid the Thread object
                # preventing garbage collection of the Client object
                args=(weakref.ref(self),),
            ).start()
        else:
            self.tracing_queue = None

        # Mount the HTTPAdapter with the retry configuration.
        adapter = requests_adapters.HTTPAdapter(max_retries=self.retry_config)
        # Don't overwrite if session already has an adapter
        if not self.session.get_adapter("http://"):
            self.session.mount("http://", adapter)
        if not self.session.get_adapter("https://"):
            self.session.mount("https://", adapter)
        self._get_data_type_cached = functools.lru_cache(maxsize=10)(
            self._get_data_type
        )
        self._anonymizer = anonymizer
        self._hide_inputs = (
            hide_inputs
            if hide_inputs is not None
            else ls_utils.get_env_var("HIDE_INPUTS") == "true"
        )
        self._hide_outputs = (
            hide_outputs
            if hide_outputs is not None
            else ls_utils.get_env_var("HIDE_OUTPUTS") == "true"
        )

    def _repr_html_(self) -> str:
        """Return an HTML representation of the instance with a link to the URL.

        Returns:
        -------
        str
            The HTML representation of the instance.
        """
        link = self._host_url
        return f'<a href="{link}", target="_blank" rel="noopener">LangSmith Client</a>'

    def __repr__(self) -> str:
        """Return a string representation of the instance with a link to the URL.

        Returns:
        -------
        str
            The string representation of the instance.
        """
        return f"Client (API URL: {self.api_url})"

    @property
    def _host(self) -> str:
        return _parse_url(self.api_url)

    @property
    def _host_url(self) -> str:
        """The web host url."""
        if self._web_url:
            link = self._web_url
        else:
            parsed_url = urllib_parse.urlparse(self.api_url)
            if _is_localhost(self.api_url):
                link = "http://localhost"
            elif parsed_url.path.endswith("/api"):
                new_path = parsed_url.path.rsplit("/api", 1)[0]
                link = urllib_parse.urlunparse(parsed_url._replace(path=new_path))
            elif parsed_url.netloc.startswith("dev."):
                link = "https://dev.smith.langchain.com"
            else:
                link = "https://smith.langchain.com"
        return link

    @property
    def _headers(self) -> Dict[str, str]:
        """Get the headers for the API request.

        Returns:
        -------
        Dict[str, str]
            The headers for the API request.
        """
        headers = {
            "User-Agent": f"langsmith-py/{langsmith.__version__}",
            "Accept": "application/json",
        }
        if self.api_key:
            headers[X_API_KEY] = self.api_key
        return headers

    @property
    def info(self) -> ls_schemas.LangSmithInfo:
        """Get the information about the LangSmith API.

        Returns:
        -------
        Optional[ls_schemas.LangSmithInfo]
            The information about the LangSmith API, or None if the API is
                not available.
        """
        if self._info is None:
            try:
                response = self.request_with_retries(
                    "GET",
                    "/info",
                    headers={"Accept": "application/json"},
                    timeout=(self.timeout_ms[0] / 1000, self.timeout_ms[1] / 1000),
                )
                ls_utils.raise_for_status_with_text(response)
                self._info = ls_schemas.LangSmithInfo(**response.json())
            except BaseException as e:
                logger.warning(
                    f"Failed to get info from {self.api_url}: {repr(e)}",
                )
                self._info = ls_schemas.LangSmithInfo()
        return self._info

    def request_with_retries(
        self,
        /,
        method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"],
        pathname: str,
        *,
        request_kwargs: Optional[Mapping] = None,
        stop_after_attempt: int = 1,
        retry_on: Optional[Sequence[Type[BaseException]]] = None,
        to_ignore: Optional[Sequence[Type[BaseException]]] = None,
        handle_response: Optional[Callable[[requests.Response, int], Any]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send a request with retries.

        Parameters
        ----------
        request_method : str
            The HTTP request method.
        pathname : str
            The pathname of the request URL. Will be appended to the API URL.
        request_kwargs : Mapping
            Additional request parameters.
        stop_after_attempt : int, default=1
            The number of attempts to make.
        retry_on : Sequence[Type[BaseException]] or None, default=None
            The exceptions to retry on. In addition to:
            [LangSmithConnectionError, LangSmithAPIError].
        to_ignore : Sequence[Type[BaseException]] or None, default=None
            The exceptions to ignore / pass on.
        handle_response : Callable[[requests.Response, int], Any] or None, default=None
            A function to handle the response and return whether to continue
            retrying.
        **kwargs : Any
            Additional keyword arguments to pass to the request.

        Returns:
        -------
        Response
            The response object.

        Raises:
        ------
        LangSmithAPIError
            If a server error occurs.
        LangSmithUserError
            If the request fails.
        LangSmithConnectionError
            If a connection error occurs.
        LangSmithError
            If the request fails.
        """
        request_kwargs = request_kwargs or {}
        request_kwargs = {
            "timeout": (self.timeout_ms[0] / 1000, self.timeout_ms[1] / 1000),
            **request_kwargs,
            **kwargs,
            "headers": {
                **self._headers,
                **request_kwargs.get("headers", {}),
                **kwargs.get("headers", {}),
            },
        }
        if (
            method != "GET"
            and "data" in request_kwargs
            and "files" not in request_kwargs
            and not request_kwargs["headers"].get("Content-Type")
        ):
            request_kwargs["headers"]["Content-Type"] = "application/json"
        logging_filters = [
            ls_utils.FilterLangSmithRetry(),
            ls_utils.FilterPoolFullWarning(host=str(self._host)),
        ]
        retry_on_: Tuple[Type[BaseException], ...] = (
            *(retry_on or []),
            *(
                ls_utils.LangSmithConnectionError,
                ls_utils.LangSmithAPIError,
            ),
        )
        to_ignore_: Tuple[Type[BaseException], ...] = (*(to_ignore or ()),)
        response = None

        for idx in range(stop_after_attempt):
            try:
                try:
                    with ls_utils.filter_logs(_urllib3_logger, logging_filters):
                        response = self.session.request(
                            method,
                            (
                                self.api_url + pathname
                                if not pathname.startswith("http")
                                else pathname
                            ),
                            stream=False,
                            **request_kwargs,
                        )
                    ls_utils.raise_for_status_with_text(response)
                    return response
                except requests.exceptions.ReadTimeout as e:
                    logger.debug("Passing on exception %s", e)
                    if idx + 1 == stop_after_attempt:
                        raise
                    sleep_time = 2**idx + (random.random() * 0.5)
                    time.sleep(sleep_time)
                    continue

                except requests.HTTPError as e:
                    if response is not None:
                        if handle_response is not None:
                            if idx + 1 < stop_after_attempt:
                                should_continue = handle_response(response, idx + 1)
                                if should_continue:
                                    continue
                        if response.status_code == 500:
                            raise ls_utils.LangSmithAPIError(
                                f"Server error caused failure to {method}"
                                f" {pathname} in"
                                f" LangSmith API. {repr(e)}"
                            )
                        elif response.status_code == 429:
                            raise ls_utils.LangSmithRateLimitError(
                                f"Rate limit exceeded for {pathname}. {repr(e)}"
                            )
                        elif response.status_code == 401:
                            raise ls_utils.LangSmithAuthError(
                                f"Authentication failed for {pathname}. {repr(e)}"
                            )
                        elif response.status_code == 404:
                            raise ls_utils.LangSmithNotFoundError(
                                f"Resource not found for {pathname}. {repr(e)}"
                            )
                        elif response.status_code == 409:
                            raise ls_utils.LangSmithConflictError(
                                f"Conflict for {pathname}. {repr(e)}"
                            )
                        else:
                            raise ls_utils.LangSmithError(
                                f"Failed to {method} {pathname} in LangSmith"
                                f" API. {repr(e)}"
                            )

                    else:
                        raise ls_utils.LangSmithUserError(
                            f"Failed to {method} {pathname} in LangSmith API."
                            f" {repr(e)}"
                        )
                except requests.ConnectionError as e:
                    recommendation = (
                        "Please confirm your LANGCHAIN_ENDPOINT"
                        if self.api_url != "https://api.smith.langchain.com"
                        else "Please confirm your internet connection."
                    )
                    raise ls_utils.LangSmithConnectionError(
                        f"Connection error caused failure to {method} {pathname}"
                        f"  in LangSmith API. {recommendation}."
                        f" {repr(e)}"
                    ) from e
                except Exception as e:
                    args = list(e.args)
                    msg = args[1] if len(args) > 1 else ""
                    msg = msg.replace("session", "session (project)")
                    if args:
                        emsg = "\n".join(
                            [str(args[0])]
                            + [msg]
                            + [str(arg) for arg in (args[2:] if len(args) > 2 else [])]
                        )
                    else:
                        emsg = msg
                    raise ls_utils.LangSmithError(
                        f"Failed to {method} {pathname} in LangSmith API. {emsg}"
                    ) from e
            except to_ignore_ as e:
                if response is not None:
                    logger.debug("Passing on exception %s", e)
                    return response
            except ls_utils.LangSmithRateLimitError:
                if idx + 1 == stop_after_attempt:
                    raise
                if response is not None:
                    try:
                        retry_after = float(response.headers.get("retry-after", "30"))
                    except Exception as e:
                        logger.warning(
                            "Invalid retry-after header: %s",
                            repr(e),
                        )
                        retry_after = 30
                # Add exponential backoff
                retry_after = retry_after * 2**idx + random.random()
                time.sleep(retry_after)
            except retry_on_:
                # Handle other exceptions more immediately
                if idx + 1 == stop_after_attempt:
                    raise
                sleep_time = 2**idx + (random.random() * 0.5)
                time.sleep(sleep_time)
                continue
            # Else we still raise an error

        raise ls_utils.LangSmithError(
            f"Failed to {method} {pathname} in LangSmith API."
        )

    def _get_paginated_list(
        self, path: str, *, params: Optional[dict] = None
    ) -> Iterator[dict]:
        """Get a paginated list of items.

        Parameters
        ----------
        path : str
            The path of the request URL.
        params : dict or None, default=None
            The query parameters.

        Yields:
        ------
        dict
            The items in the paginated list.
        """
        params_ = params.copy() if params else {}
        offset = params_.get("offset", 0)
        params_["limit"] = params_.get("limit", 100)
        while True:
            params_["offset"] = offset
            response = self.request_with_retries(
                "GET",
                path,
                params=params_,
            )
            items = response.json()

            if not items:
                break
            yield from items
            if len(items) < params_["limit"]:
                # offset and limit isn't respected if we're
                # querying for specific values
                break
            offset += len(items)

    def _get_cursor_paginated_list(
        self,
        path: str,
        *,
        body: Optional[dict] = None,
        request_method: Literal["GET", "POST"] = "POST",
        data_key: str = "runs",
    ) -> Iterator[dict]:
        """Get a cursor paginated list of items.

        Parameters
        ----------
        path : str
            The path of the request URL.
        body : dict or None, default=None
            The query body.
        request_method : str, default="post"
            The HTTP request method.
        data_key : str, default="runs"

        Yields:
        ------
        dict
            The items in the paginated list.
        """
        params_ = body.copy() if body else {}
        while True:
            response = self.request_with_retries(
                request_method,
                path,
                request_kwargs={
                    "data": _dumps_json(params_),
                },
            )
            response_body = response.json()
            if not response_body:
                break
            if not response_body.get(data_key):
                break
            yield from response_body[data_key]
            cursors = response_body.get("cursors")
            if not cursors:
                break
            if not cursors.get("next"):
                break
            params_["cursor"] = cursors["next"]

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        name: str,
        input_keys: Sequence[str],
        output_keys: Sequence[str],
        *,
        description: Optional[str] = None,
        data_type: Optional[ls_schemas.DataType] = ls_schemas.DataType.kv,
    ) -> ls_schemas.Dataset:
        """Upload a dataframe as individual examples to the LangSmith API.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to upload.
        name : str
            The name of the dataset.
        input_keys : Sequence[str]
            The input keys.
        output_keys : Sequence[str]
            The output keys.
        description : str or None, default=None
            The description of the dataset.
        data_type : DataType or None, default=DataType.kv
            The data type of the dataset.

        Returns:
        -------
        Dataset
            The uploaded dataset.

        Raises:
        ------
        ValueError
            If the csv_file is not a string or tuple.
        """
        csv_file = io.BytesIO()
        df.to_csv(csv_file, index=False)
        csv_file.seek(0)
        return self.upload_csv(
            ("data.csv", csv_file),
            input_keys=input_keys,
            output_keys=output_keys,
            description=description,
            name=name,
            data_type=data_type,
        )

    def upload_csv(
        self,
        csv_file: Union[str, Tuple[str, io.BytesIO]],
        input_keys: Sequence[str],
        output_keys: Sequence[str],
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        data_type: Optional[ls_schemas.DataType] = ls_schemas.DataType.kv,
    ) -> ls_schemas.Dataset:
        """Upload a CSV file to the LangSmith API.

        Parameters
        ----------
        csv_file : str or Tuple[str, BytesIO]
            The CSV file to upload. If a string, it should be the path
            If a tuple, it should be a tuple containing the filename
            and a BytesIO object.
        input_keys : Sequence[str]
            The input keys.
        output_keys : Sequence[str]
            The output keys.
        name : str or None, default=None
            The name of the dataset.
        description : str or None, default=None
            The description of the dataset.
        data_type : DataType or None, default=DataType.kv
            The data type of the dataset.

        Returns:
        -------
        Dataset
            The uploaded dataset.

        Raises:
        ------
        ValueError
            If the csv_file is not a string or tuple.
        """
        data = {
            "input_keys": input_keys,
            "output_keys": output_keys,
        }
        if name:
            data["name"] = name
        if description:
            data["description"] = description
        if data_type:
            data["data_type"] = ls_utils.get_enum_value(data_type)
        data["id"] = str(uuid.uuid4())
        if isinstance(csv_file, str):
            with open(csv_file, "rb") as f:
                file_ = {"file": f}
                response = self.request_with_retries(
                    "POST",
                    "/datasets/upload",
                    data=data,
                    files=file_,
                )
        elif isinstance(csv_file, tuple):
            response = self.request_with_retries(
                "POST",
                "/datasets/upload",
                data=data,
                files={"file": csv_file},
            )
        else:
            raise ValueError("csv_file must be a string or tuple")
        ls_utils.raise_for_status_with_text(response)
        result = response.json()
        # TODO: Make this more robust server-side
        if "detail" in result and "already exists" in result["detail"]:
            file_name = csv_file if isinstance(csv_file, str) else csv_file[0]
            file_name = file_name.split("/")[-1]
            raise ValueError(f"Dataset {file_name} already exists")
        return ls_schemas.Dataset(
            **result,
            _host_url=self._host_url,
            _tenant_id=self._get_optional_tenant_id(),
        )

    def _run_transform(
        self,
        run: Union[ls_schemas.Run, dict, ls_schemas.RunLikeDict],
        update: bool = False,
        copy: bool = False,
    ) -> dict:
        """Transform the given run object into a dictionary representation.

        Args:
            run (Union[ls_schemas.Run, dict]): The run object to transform.
            update (bool, optional): Whether to update the run. Defaults to False.
            copy (bool, optional): Whether to copy the run. Defaults to False.

        Returns:
            dict: The transformed run object as a dictionary.
        """
        if hasattr(run, "dict") and callable(getattr(run, "dict")):
            run_create: dict = run.dict()  # type: ignore
        else:
            run_create = cast(dict, run)
        if "id" not in run_create:
            run_create["id"] = uuid.uuid4()
        elif isinstance(run_create["id"], str):
            run_create["id"] = uuid.UUID(run_create["id"])
        if "inputs" in run_create and run_create["inputs"] is not None:
            if copy:
                run_create["inputs"] = ls_utils.deepish_copy(run_create["inputs"])
            run_create["inputs"] = self._hide_run_inputs(run_create["inputs"])
        if "outputs" in run_create and run_create["outputs"] is not None:
            if copy:
                run_create["outputs"] = ls_utils.deepish_copy(run_create["outputs"])
            run_create["outputs"] = self._hide_run_outputs(run_create["outputs"])
        if not update and not run_create.get("start_time"):
            run_create["start_time"] = datetime.datetime.now(datetime.timezone.utc)
        return run_create

    @staticmethod
    def _insert_runtime_env(runs: Sequence[dict]) -> None:
        runtime_env = ls_env.get_runtime_and_metrics()
        for run_create in runs:
            run_extra = cast(dict, run_create.setdefault("extra", {}))
            # update runtime
            runtime: dict = run_extra.setdefault("runtime", {})
            run_extra["runtime"] = {**runtime_env, **runtime}
            # update metadata
            metadata: dict = run_extra.setdefault("metadata", {})
            langchain_metadata = ls_env.get_langchain_env_var_metadata()
            metadata.update(
                {k: v for k, v in langchain_metadata.items() if k not in metadata}
            )

    def _filter_for_sampling(
        self, runs: Iterable[dict], *, patch: bool = False
    ) -> list[dict]:
        if self.tracing_sample_rate is None:
            return list(runs)

        if patch:
            sampled = []
            for run in runs:
                run_id = _as_uuid(run["id"])
                if run_id in self._sampled_post_uuids:
                    sampled.append(run)
                    self._sampled_post_uuids.remove(run_id)
            return sampled
        else:
            sampled = []
            for run in runs:
                if random.random() < self.tracing_sample_rate:
                    sampled.append(run)
                    self._sampled_post_uuids.add(_as_uuid(run["id"]))
            return sampled

    def create_run(
        self,
        name: str,
        inputs: Dict[str, Any],
        run_type: RUN_TYPE_T,
        *,
        project_name: Optional[str] = None,
        revision_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Persist a run to the LangSmith API.

        Parameters
        ----------
        name : str
            The name of the run.
        inputs : Dict[str, Any]
            The input values for the run.
        run_type : str
            The type of the run, such as tool, chain, llm, retriever,
            embedding, prompt, or parser.
        revision_id : ID_TYPE or None, default=None
            The revision ID of the run.
        **kwargs : Any
            Additional keyword arguments.

        Raises:
        ------
        LangSmithUserError
            If the API key is not provided when using the hosted service.
        """
        project_name = project_name or kwargs.pop(
            "session_name",
            # if the project is not provided, use the environment's project
            ls_utils.get_tracer_project(),
        )
        run_create = {
            **kwargs,
            "session_name": project_name,
            "name": name,
            "inputs": inputs,
            "run_type": run_type,
        }
        if not self._filter_for_sampling([run_create]):
            return
        run_create = self._run_transform(run_create, copy=True)
        self._insert_runtime_env([run_create])
        if revision_id is not None:
            run_create["extra"]["metadata"]["revision_id"] = revision_id
        if (
            self.tracing_queue is not None
            # batch ingest requires trace_id and dotted_order to be set
            and run_create.get("trace_id") is not None
            and run_create.get("dotted_order") is not None
        ):
            return self.tracing_queue.put(
                TracingQueueItem(run_create["dotted_order"], "create", run_create)
            )
        self._create_run(run_create)

    def _create_run(self, run_create: dict):
        for api_url, api_key in self._write_api_urls.items():
            headers = {**self._headers, X_API_KEY: api_key}
            self.request_with_retries(
                "POST",
                f"{api_url}/runs",
                request_kwargs={
                    "data": _dumps_json(run_create),
                    "headers": headers,
                },
                to_ignore=(ls_utils.LangSmithConflictError,),
            )

    def _hide_run_inputs(self, inputs: dict):
        if self._hide_inputs is True:
            return {}
        if self._anonymizer:
            json_inputs = orjson.loads(_dumps_json(inputs))
            return self._anonymizer(json_inputs)
        if self._hide_inputs is False:
            return inputs
        return self._hide_inputs(inputs)

    def _hide_run_outputs(self, outputs: dict):
        if self._hide_outputs is True:
            return {}
        if self._anonymizer:
            json_outputs = orjson.loads(_dumps_json(outputs))
            return self._anonymizer(json_outputs)
        if self._hide_outputs is False:
            return outputs
        return self._hide_outputs(outputs)

    def batch_ingest_runs(
        self,
        create: Optional[
            Sequence[Union[ls_schemas.Run, ls_schemas.RunLikeDict, Dict]]
        ] = None,
        update: Optional[
            Sequence[Union[ls_schemas.Run, ls_schemas.RunLikeDict, Dict]]
        ] = None,
        *,
        pre_sampled: bool = False,
    ):
        """Batch ingest/upsert multiple runs in the Langsmith system.

        Args:
            create (Optional[Sequence[Union[ls_schemas.Run, RunLikeDict]]]):
                A sequence of `Run` objects or equivalent dictionaries representing
                runs to be created / posted.
            update (Optional[Sequence[Union[ls_schemas.Run, RunLikeDict]]]):
                A sequence of `Run` objects or equivalent dictionaries representing
                runs that have already been created and should be updated / patched.
            pre_sampled (bool, optional): Whether the runs have already been subject
                to sampling, and therefore should not be sampled again.
                Defaults to False.

        Returns:
            None: If both `create` and `update` are None.

        Raises:
            LangsmithAPIError: If there is an error in the API request.

        Note:
            - The run objects MUST contain the dotted_order and trace_id fields
                to be accepted by the API.
        """
        if not create and not update:
            return
        # transform and convert to dicts
        create_dicts = [self._run_transform(run) for run in create or []]
        update_dicts = [self._run_transform(run, update=True) for run in update or []]
        # combine post and patch dicts where possible
        if update_dicts and create_dicts:
            create_by_id = {run["id"]: run for run in create_dicts}
            standalone_updates: list[dict] = []
            for run in update_dicts:
                if run["id"] in create_by_id:
                    create_by_id[run["id"]].update(
                        {k: v for k, v in run.items() if v is not None}
                    )
                else:
                    standalone_updates.append(run)
            update_dicts = standalone_updates
        for run in create_dicts:
            if not run.get("trace_id") or not run.get("dotted_order"):
                raise ls_utils.LangSmithUserError(
                    "Batch ingest requires trace_id and dotted_order to be set."
                )
        for run in update_dicts:
            if not run.get("trace_id") or not run.get("dotted_order"):
                raise ls_utils.LangSmithUserError(
                    "Batch ingest requires trace_id and dotted_order to be set."
                )
        # filter out runs that are not sampled
        if pre_sampled:
            raw_body = {
                "post": create_dicts,
                "patch": update_dicts,
            }
        else:
            raw_body = {
                "post": self._filter_for_sampling(create_dicts),
                "patch": self._filter_for_sampling(update_dicts, patch=True),
            }
        if not raw_body["post"] and not raw_body["patch"]:
            return

        self._insert_runtime_env(raw_body["post"] + raw_body["patch"])
        info = self.info

        size_limit_bytes = (info.batch_ingest_config or {}).get(
            "size_limit_bytes"
            # 20 MB max by default
        ) or 20_971_520
        # Get orjson fragments to avoid going over the max request size
        partial_body = {
            "post": [_dumps_json(run) for run in raw_body["post"]],
            "patch": [_dumps_json(run) for run in raw_body["patch"]],
        }
        body_chunks: DefaultDict[str, list] = collections.defaultdict(list)
        body_size = 0
        for key in ["post", "patch"]:
            body = collections.deque(partial_body[key])
            while body:
                if body_size > 0 and body_size + len(body[0]) > size_limit_bytes:
                    self._post_batch_ingest_runs(orjson.dumps(body_chunks))
                    body_size = 0
                    body_chunks.clear()
                body_size += len(body[0])
                body_chunks[key].append(orjson.Fragment(body.popleft()))
        if body_size:
            self._post_batch_ingest_runs(orjson.dumps(body_chunks))

    def _post_batch_ingest_runs(self, body: bytes):
        try:
            for api_url, api_key in self._write_api_urls.items():
                self.request_with_retries(
                    "POST",
                    f"{api_url}/runs/batch",
                    request_kwargs={
                        "data": body,
                        "headers": {
                            **self._headers,
                            X_API_KEY: api_key,
                        },
                    },
                    to_ignore=(ls_utils.LangSmithConflictError,),
                    stop_after_attempt=3,
                )
        except Exception as e:
            logger.warning(f"Failed to batch ingest runs: {repr(e)}")

    def update_run(
        self,
        run_id: ID_TYPE,
        *,
        end_time: Optional[datetime.datetime] = None,
        error: Optional[str] = None,
        inputs: Optional[Dict] = None,
        outputs: Optional[Dict] = None,
        events: Optional[Sequence[dict]] = None,
        extra: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Update a run in the LangSmith API.

        Parameters
        ----------
        run_id : str or UUID
            The ID of the run to update.
        end_time : datetime or None
            The end time of the run.
        error : str or None, default=None
            The error message of the run.
        inputs : Dict or None, default=None
            The input values for the run.
        outputs : Dict or None, default=None
            The output values for the run.
        events : Sequence[dict] or None, default=None
            The events for the run.
        extra : Dict or None, default=None
            The extra information for the run.
        tags : List[str] or None, default=None
            The tags for the run.
        **kwargs : Any
            Kwargs are ignored.
        """
        data: Dict[str, Any] = {
            "id": _as_uuid(run_id, "run_id"),
            "trace_id": kwargs.pop("trace_id", None),
            "parent_run_id": kwargs.pop("parent_run_id", None),
            "dotted_order": kwargs.pop("dotted_order", None),
            "tags": tags,
            "extra": extra,
        }
        if not self._filter_for_sampling([data], patch=True):
            return
        if end_time is not None:
            data["end_time"] = end_time.isoformat()
        else:
            data["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        if error is not None:
            data["error"] = error
        if inputs is not None:
            data["inputs"] = self._hide_run_inputs(inputs)
        if outputs is not None:
            outputs = ls_utils.deepish_copy(outputs)
            data["outputs"] = self._hide_run_outputs(outputs)
        if events is not None:
            data["events"] = events
        if (
            self.tracing_queue is not None
            # batch ingest requires trace_id and dotted_order to be set
            and data["trace_id"] is not None
            and data["dotted_order"] is not None
        ):
            return self.tracing_queue.put(
                TracingQueueItem(data["dotted_order"], "update", data)
            )
        return self._update_run(data)

    def _update_run(self, run_update: dict) -> None:
        for api_url, api_key in self._write_api_urls.items():
            headers = {
                **self._headers,
                X_API_KEY: api_key,
            }

            self.request_with_retries(
                "PATCH",
                f"{api_url}/runs/{run_update['id']}",
                request_kwargs={
                    "data": _dumps_json(run_update),
                    "headers": headers,
                },
            )

    def _load_child_runs(self, run: ls_schemas.Run) -> ls_schemas.Run:
        """Load child runs for a given run.

        Parameters
        ----------
        run : Run
            The run to load child runs for.

        Returns:
        -------
        Run
            The run with loaded child runs.

        Raises:
        ------
        LangSmithError
            If a child run has no parent.
        """
        child_runs = self.list_runs(id=run.child_run_ids)
        treemap: DefaultDict[uuid.UUID, List[ls_schemas.Run]] = collections.defaultdict(
            list
        )
        runs: Dict[uuid.UUID, ls_schemas.Run] = {}
        for child_run in sorted(
            child_runs,
            key=lambda r: r.dotted_order,
        ):
            if child_run.parent_run_id is None:
                raise ls_utils.LangSmithError(f"Child run {child_run.id} has no parent")
            treemap[child_run.parent_run_id].append(child_run)
            runs[child_run.id] = child_run
        run.child_runs = treemap.pop(run.id, [])
        for run_id, children in treemap.items():
            runs[run_id].child_runs = children
        return run

    def read_run(
        self, run_id: ID_TYPE, load_child_runs: bool = False
    ) -> ls_schemas.Run:
        """Read a run from the LangSmith API.

        Parameters
        ----------
        run_id : str or UUID
            The ID of the run to read.
        load_child_runs : bool, default=False
            Whether to load nested child runs.

        Returns:
        -------
        Run
            The run.
        """
        response = self.request_with_retries(
            "GET", f"/runs/{_as_uuid(run_id, 'run_id')}"
        )
        run = ls_schemas.Run(**response.json(), _host_url=self._host_url)
        if load_child_runs and run.child_run_ids:
            run = self._load_child_runs(run)
        return run

    def list_runs(
        self,
        *,
        project_id: Optional[Union[ID_TYPE, Sequence[ID_TYPE]]] = None,
        project_name: Optional[Union[str, Sequence[str]]] = None,
        run_type: Optional[str] = None,
        trace_id: Optional[ID_TYPE] = None,
        reference_example_id: Optional[ID_TYPE] = None,
        query: Optional[str] = None,
        filter: Optional[str] = None,
        trace_filter: Optional[str] = None,
        tree_filter: Optional[str] = None,
        is_root: Optional[bool] = None,
        parent_run_id: Optional[ID_TYPE] = None,
        start_time: Optional[datetime.datetime] = None,
        error: Optional[bool] = None,
        run_ids: Optional[Sequence[ID_TYPE]] = None,
        select: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[ls_schemas.Run]:
        """List runs from the LangSmith API.

        Parameters
        ----------
        project_id : UUID or None, default=None
            The ID(s) of the project to filter by.
        project_name : str or None, default=None
            The name(s) of the project to filter by.
        run_type : str or None, default=None
            The type of the runs to filter by.
        trace_id : UUID or None, default=None
            The ID of the trace to filter by.
        reference_example_id : UUID or None, default=None
            The ID of the reference example to filter by.
        query : str or None, default=None
            The query string to filter by.
        filter : str or None, default=None
            The filter string to filter by.
        trace_filter : str or None, default=None
            Filter to apply to the ROOT run in the trace tree. This is meant to
            be used in conjunction with the regular `filter` parameter to let you
            filter runs by attributes of the root run within a trace.
        tree_filter : str or None, default=None
            Filter to apply to OTHER runs in the trace tree, including
            sibling and child runs. This is meant to be used in conjunction with
            the regular `filter` parameter to let you filter runs by attributes
            of any run within a trace.
        is_root : bool or None, default=None
            Whether to filter by root runs.
        parent_run_id : UUID or None, default=None
            The ID of the parent run to filter by.
        start_time : datetime or None, default=None
            The start time to filter by.
        error : bool or None, default=None
            Whether to filter by error status.
        run_ids : List[str or UUID] or None, default=None
            The IDs of the runs to filter by.
        limit : int or None, default=None
            The maximum number of runs to return.
        **kwargs : Any
            Additional keyword arguments.

        Yields:
        ------
        Run
            The runs.

        Examples:
        --------
        .. code-block:: python

            # List all runs in a project
            project_runs = client.list_runs(project_name="<your_project>")

            # List LLM and Chat runs in the last 24 hours
            todays_llm_runs = client.list_runs(
                project_name="<your_project>",
                start_time=datetime.now() - timedelta(days=1),
                run_type="llm",
            )

            # List root traces in a project
            root_runs = client.list_runs(project_name="<your_project>", is_root=1)

            # List runs without errors
            correct_runs = client.list_runs(project_name="<your_project>", error=False)

            # List runs and only return their inputs/outputs (to speed up the query)
            input_output_runs = client.list_runs(
                project_name="<your_project>", select=["inputs", "outputs"]
            )

            # List runs by run ID
            run_ids = [
                "a36092d2-4ad5-4fb4-9c0d-0dba9a2ed836",
                "9398e6be-964f-4aa4-8ae9-ad78cd4b7074",
            ]
            selected_runs = client.list_runs(id=run_ids)

            # List all "chain" type runs that took more than 10 seconds and had
            # `total_tokens` greater than 5000
            chain_runs = client.list_runs(
                project_name="<your_project>",
                filter='and(eq(run_type, "chain"), gt(latency, 10), gt(total_tokens, 5000))',
            )

            # List all runs called "extractor" whose root of the trace was assigned feedback "user_score" score of 1
            good_extractor_runs = client.list_runs(
                project_name="<your_project>",
                filter='eq(name, "extractor")',
                trace_filter='and(eq(feedback_key, "user_score"), eq(feedback_score, 1))',
            )

            # List all runs that started after a specific timestamp and either have "error" not equal to null or a "Correctness" feedback score equal to 0
            complex_runs = client.list_runs(
                project_name="<your_project>",
                filter='and(gt(start_time, "2023-07-15T12:34:56Z"), or(neq(error, null), and(eq(feedback_key, "Correctness"), eq(feedback_score, 0.0))))',
            )

            # List all runs where `tags` include "experimental" or "beta" and `latency` is greater than 2 seconds
            tagged_runs = client.list_runs(
                project_name="<your_project>",
                filter='and(or(has(tags, "experimental"), has(tags, "beta")), gt(latency, 2))',
            )
        """  # noqa: E501
        project_ids = []
        if isinstance(project_id, (uuid.UUID, str)):
            project_ids.append(project_id)
        elif isinstance(project_id, list):
            project_ids.extend(project_id)
        if project_name is not None:
            if isinstance(project_name, str):
                project_name = [project_name]
            project_ids.extend(
                [self.read_project(project_name=name).id for name in project_name]
            )
        default_select = [
            "app_path",
            "child_run_ids",
            "completion_cost",
            "completion_tokens",
            "dotted_order",
            "end_time",
            "error",
            "events",
            "extra",
            "feedback_stats",
            "first_token_time",
            "id",
            "inputs",
            "name",
            "outputs",
            "parent_run_id",
            "parent_run_ids",
            "prompt_cost",
            "prompt_tokens",
            "reference_example_id",
            "run_type",
            "session_id",
            "start_time",
            "status",
            "tags",
            "total_cost",
            "total_tokens",
            "trace_id",
        ]
        select = select or default_select
        body_query: Dict[str, Any] = {
            "session": project_ids if project_ids else None,
            "run_type": run_type,
            "reference_example": (
                [reference_example_id] if reference_example_id else None
            ),
            "query": query,
            "filter": filter,
            "trace_filter": trace_filter,
            "tree_filter": tree_filter,
            "is_root": is_root,
            "parent_run": parent_run_id,
            "start_time": start_time.isoformat() if start_time else None,
            "error": error,
            "id": run_ids,
            "trace": trace_id,
            "select": select,
            **kwargs,
        }
        body_query = {k: v for k, v in body_query.items() if v is not None}
        for i, run in enumerate(
            self._get_cursor_paginated_list("/runs/query", body=body_query)
        ):
            yield ls_schemas.Run(**run, _host_url=self._host_url)
            if limit is not None and i + 1 >= limit:
                break

    def get_run_url(
        self,
        *,
        run: ls_schemas.RunBase,
        project_name: Optional[str] = None,
        project_id: Optional[ID_TYPE] = None,
    ) -> str:
        """Get the URL for a run.

        Parameters
        ----------
        run : Run
            The run.
        project_name : str or None, default=None
            The name of the project.
        project_id : UUID or None, default=None
            The ID of the project.

        Returns:
        -------
        str
            The URL for the run.
        """
        if hasattr(run, "session_id") and run.session_id is not None:
            session_id = run.session_id
        elif project_id is not None:
            session_id = project_id
        elif project_name is not None:
            session_id = self.read_project(project_name=project_name).id
        else:
            project_name = ls_utils.get_tracer_project()
            session_id = self.read_project(project_name=project_name).id
        session_id_ = _as_uuid(session_id, "session_id")
        return (
            f"{self._host_url}/o/{self._get_tenant_id()}/projects/p/{session_id_}/"
            f"r/{run.id}?poll=true"
        )

    def share_run(self, run_id: ID_TYPE, *, share_id: Optional[ID_TYPE] = None) -> str:
        """Get a share link for a run."""
        run_id_ = _as_uuid(run_id, "run_id")
        data = {
            "run_id": str(run_id_),
            "share_token": share_id or str(uuid.uuid4()),
        }
        response = self.request_with_retries(
            "PUT",
            f"/runs/{run_id_}/share",
            headers=self._headers,
            json=data,
        )
        ls_utils.raise_for_status_with_text(response)
        share_token = response.json()["share_token"]
        return f"{self._host_url}/public/{share_token}/r"

    def unshare_run(self, run_id: ID_TYPE) -> None:
        """Delete share link for a run."""
        response = self.request_with_retries(
            "DELETE",
            f"/runs/{_as_uuid(run_id, 'run_id')}/share",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def read_run_shared_link(self, run_id: ID_TYPE) -> Optional[str]:
        """Retrieve the shared link for a specific run.

        Args:
            run_id (ID_TYPE): The ID of the run.

        Returns:
            Optional[str]: The shared link for the run, or None if the link is not
            available.
        """
        response = self.request_with_retries(
            "GET",
            f"/runs/{_as_uuid(run_id, 'run_id')}/share",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)
        result = response.json()
        if result is None or "share_token" not in result:
            return None
        return f"{self._host_url}/public/{result['share_token']}/r"

    def run_is_shared(self, run_id: ID_TYPE) -> bool:
        """Get share state for a run."""
        link = self.read_run_shared_link(_as_uuid(run_id, "run_id"))
        return link is not None

    def list_shared_runs(
        self, share_token: ID_TYPE, run_ids: Optional[List[str]] = None
    ) -> List[ls_schemas.Run]:
        """Get shared runs."""
        params = {"id": run_ids, "share_token": str(share_token)}
        response = self.request_with_retries(
            "GET",
            f"/public/{_as_uuid(share_token, 'share_token')}/runs",
            headers=self._headers,
            params=params,
        )
        ls_utils.raise_for_status_with_text(response)
        return [
            ls_schemas.Run(**run, _host_url=self._host_url) for run in response.json()
        ]

    def read_dataset_shared_schema(
        self,
        dataset_id: Optional[ID_TYPE] = None,
        *,
        dataset_name: Optional[str] = None,
    ) -> ls_schemas.DatasetShareSchema:
        """Retrieve the shared schema of a dataset.

        Args:
            dataset_id (Optional[ID_TYPE]): The ID of the dataset.
                Either `dataset_id` or `dataset_name` must be given.
            dataset_name (Optional[str]): The name of the dataset.
                Either `dataset_id` or `dataset_name` must be given.

        Returns:
            ls_schemas.DatasetShareSchema: The shared schema of the dataset.

        Raises:
            ValueError: If neither `dataset_id` nor `dataset_name` is given.
        """
        if dataset_id is None and dataset_name is None:
            raise ValueError("Either dataset_id or dataset_name must be given")
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        response = self.request_with_retries(
            "GET",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/share",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)
        d = response.json()
        return cast(
            ls_schemas.DatasetShareSchema,
            {
                **d,
                "url": f"{self._host_url}/public/"
                f"{_as_uuid(d['share_token'], 'response.share_token')}/d",
            },
        )

    def share_dataset(
        self,
        dataset_id: Optional[ID_TYPE] = None,
        *,
        dataset_name: Optional[str] = None,
    ) -> ls_schemas.DatasetShareSchema:
        """Get a share link for a dataset."""
        if dataset_id is None and dataset_name is None:
            raise ValueError("Either dataset_id or dataset_name must be given")
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        data = {
            "dataset_id": str(dataset_id),
        }
        response = self.request_with_retries(
            "PUT",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/share",
            headers=self._headers,
            json=data,
        )
        ls_utils.raise_for_status_with_text(response)
        d: dict = response.json()
        return cast(
            ls_schemas.DatasetShareSchema,
            {**d, "url": f"{self._host_url}/public/{d['share_token']}/d"},
        )

    def unshare_dataset(self, dataset_id: ID_TYPE) -> None:
        """Delete share link for a dataset."""
        response = self.request_with_retries(
            "DELETE",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/share",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def read_shared_dataset(
        self,
        share_token: str,
    ) -> ls_schemas.Dataset:
        """Get shared datasets."""
        response = self.request_with_retries(
            "GET",
            f"/public/{_as_uuid(share_token, 'share_token')}/datasets",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.Dataset(
            **response.json(),
            _host_url=self._host_url,
            _public_path=f"/public/{share_token}/d",
        )

    def list_shared_examples(
        self, share_token: str, *, example_ids: Optional[List[ID_TYPE]] = None
    ) -> List[ls_schemas.Example]:
        """Get shared examples."""
        params = {}
        if example_ids is not None:
            params["id"] = [str(id) for id in example_ids]
        response = self.request_with_retries(
            "GET",
            f"/public/{_as_uuid(share_token, 'share_token')}/examples",
            headers=self._headers,
            params=params,
        )
        ls_utils.raise_for_status_with_text(response)
        return [
            ls_schemas.Example(**dataset, _host_url=self._host_url)
            for dataset in response.json()
        ]

    def list_shared_projects(
        self,
        *,
        dataset_share_token: str,
        project_ids: Optional[List[ID_TYPE]] = None,
        name: Optional[str] = None,
        name_contains: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.TracerSessionResult]:
        """List shared projects.

        Args:
            dataset_share_token : str
                The share token of the dataset.
            project_ids : List[ID_TYPE], optional
                List of project IDs to filter the results, by default None.
            name : str, optional
                Name of the project to filter the results, by default None.
            name_contains : str, optional
                Substring to search for in project names, by default None.
            limit : int, optional

        Yields:
            TracerSessionResult: The shared projects.
        """
        params = {"id": project_ids, "name": name, "name_contains": name_contains}
        share_token = _as_uuid(dataset_share_token, "dataset_share_token")
        for i, project in enumerate(
            self._get_paginated_list(
                f"/public/{share_token}/datasets/sessions",
                params=params,
            )
        ):
            yield ls_schemas.TracerSessionResult(**project, _host_url=self._host_url)
            if limit is not None and i + 1 >= limit:
                break

    def create_project(
        self,
        project_name: str,
        *,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        upsert: bool = False,
        project_extra: Optional[dict] = None,
        reference_dataset_id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.TracerSession:
        """Create a project on the LangSmith API.

        Parameters
        ----------
        project_name : str
            The name of the project.
        project_extra : dict or None, default=None
            Additional project information.
        metadata: dict or None, default=None
            Additional metadata to associate with the project.
        description : str or None, default=None
            The description of the project.
        upsert : bool, default=False
            Whether to update the project if it already exists.
        reference_dataset_id: UUID or None, default=None
            The ID of the reference dataset to associate with the project.

        Returns:
        -------
        TracerSession
            The created project.
        """
        endpoint = f"{self.api_url}/sessions"
        extra = project_extra
        if metadata:
            extra = {**(extra or {}), "metadata": metadata}
        body: Dict[str, Any] = {
            "name": project_name,
            "extra": extra,
            "description": description,
            "id": str(uuid.uuid4()),
        }
        params = {}
        if upsert:
            params["upsert"] = True
        if reference_dataset_id is not None:
            body["reference_dataset_id"] = reference_dataset_id
        response = self.request_with_retries(
            "POST",
            endpoint,
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json(body),
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.TracerSession(**response.json(), _host_url=self._host_url)

    def update_project(
        self,
        project_id: ID_TYPE,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[dict] = None,
        project_extra: Optional[dict] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> ls_schemas.TracerSession:
        """Update a LangSmith project.

        Parameters
        ----------
        project_id : UUID
            The ID of the project to update.
        name : str or None, default=None
            The new name to give the project. This is only valid if the project
            has been assigned an end_time, meaning it has been completed/closed.
        description : str or None, default=None
            The new description to give the project.
        metadata: dict or None, default=None

        project_extra : dict or None, default=None
            Additional project information.

        Returns:
        -------
        TracerSession
            The updated project.
        """
        endpoint = f"{self.api_url}/sessions/{_as_uuid(project_id, 'project_id')}"
        extra = project_extra
        if metadata:
            extra = {**(extra or {}), "metadata": metadata}
        body: Dict[str, Any] = {
            "name": name,
            "extra": extra,
            "description": description,
            "end_time": end_time.isoformat() if end_time else None,
        }
        response = self.request_with_retries(
            "PATCH",
            endpoint,
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json(body),
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.TracerSession(**response.json(), _host_url=self._host_url)

    def _get_optional_tenant_id(self) -> Optional[uuid.UUID]:
        if self._tenant_id is not None:
            return self._tenant_id
        try:
            response = self.request_with_retries(
                "GET", "/sessions", params={"limit": 1}
            )
            result = response.json()
            if isinstance(result, list) and len(result) > 0:
                tracer_session = ls_schemas.TracerSessionResult(
                    **result[0], _host_url=self._host_url
                )
                self._tenant_id = tracer_session.tenant_id
                return self._tenant_id
        except Exception as e:
            logger.warning(
                "Failed to get tenant ID from LangSmith: %s", repr(e), exc_info=True
            )
        return None

    def _get_tenant_id(self) -> uuid.UUID:
        tenant_id = self._get_optional_tenant_id()
        if tenant_id is None:
            raise ls_utils.LangSmithError("No tenant ID found")
        return tenant_id

    @ls_utils.xor_args(("project_id", "project_name"))
    def read_project(
        self,
        *,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        include_stats: bool = False,
    ) -> ls_schemas.TracerSessionResult:
        """Read a project from the LangSmith API.

        Parameters
        ----------
        project_id : str or None, default=None
            The ID of the project to read.
        project_name : str or None, default=None
            The name of the project to read.
                Note: Only one of project_id or project_name may be given.
        include_stats : bool, default=False
            Whether to include a project's aggregate statistics in the response.

        Returns:
        -------
        TracerSessionResult
            The project.
        """
        path = "/sessions"
        params: Dict[str, Any] = {"limit": 1}
        if project_id is not None:
            path += f"/{_as_uuid(project_id, 'project_id')}"
        elif project_name is not None:
            params["name"] = project_name
        else:
            raise ValueError("Must provide project_name or project_id")
        params["include_stats"] = include_stats
        response = self.request_with_retries("GET", path, params=params)
        result = response.json()
        if isinstance(result, list):
            if len(result) == 0:
                raise ls_utils.LangSmithNotFoundError(
                    f"Project {project_name} not found"
                )
            return ls_schemas.TracerSessionResult(**result[0], _host_url=self._host_url)
        return ls_schemas.TracerSessionResult(
            **response.json(), _host_url=self._host_url
        )

    def has_project(
        self, project_name: str, *, project_id: Optional[str] = None
    ) -> bool:
        """Check if a project exists.

        Parameters
        ----------
        project_name : str
            The name of the project to check for.
        project_id : str or None, default=None
            The ID of the project to check for.

        Returns:
        -------
        bool
            Whether the project exists.
        """
        try:
            self.read_project(project_name=project_name)
        except ls_utils.LangSmithNotFoundError:
            return False
        return True

    def get_test_results(
        self,
        *,
        project_id: Optional[ID_TYPE] = None,
        project_name: Optional[str] = None,
    ) -> "pd.DataFrame":
        """Read the record-level information from an experiment into a Pandas DF.

        Note: this will fetch whatever data exists in the DB. Results are not
        immediately available in the DB upon evaluation run completion.

        Returns:
        -------
        pd.DataFrame
            A dataframe containing the test results.
        """
        warnings.warn(
            "Function get_test_results is in beta.", UserWarning, stacklevel=2
        )
        from concurrent.futures import ThreadPoolExecutor, as_completed  # type: ignore

        import pandas as pd  # type: ignore

        runs = self.list_runs(
            project_id=project_id,
            project_name=project_name,
            is_root=True,
            select=[
                "id",
                "reference_example_id",
                "inputs",
                "outputs",
                "error",
                "feedback_stats",
                "start_time",
                "end_time",
            ],
        )
        results: list[dict] = []
        example_ids = []

        def fetch_examples(batch):
            examples = self.list_examples(example_ids=batch)
            return [
                {
                    "example_id": example.id,
                    **{f"reference.{k}": v for k, v in (example.outputs or {}).items()},
                }
                for example in examples
            ]

        batch_size = 50
        cursor = 0
        with ThreadPoolExecutor() as executor:
            futures = []
            for r in runs:
                row = {
                    "example_id": r.reference_example_id,
                    **{f"input.{k}": v for k, v in r.inputs.items()},
                    **{f"outputs.{k}": v for k, v in (r.outputs or {}).items()},
                    "execution_time": (
                        (r.end_time - r.start_time).total_seconds()
                        if r.end_time
                        else None
                    ),
                    "error": r.error,
                    "id": r.id,
                }
                if r.feedback_stats:
                    row.update(
                        {
                            f"feedback.{k}": v.get("avg")
                            for k, v in r.feedback_stats.items()
                        }
                    )
                if r.reference_example_id:
                    example_ids.append(r.reference_example_id)
                else:
                    logger.warning(f"Run {r.id} has no reference example ID.")
                if len(example_ids) % batch_size == 0:
                    # Ensure not empty
                    if batch := example_ids[cursor : cursor + batch_size]:
                        futures.append(executor.submit(fetch_examples, batch))
                        cursor += batch_size
                results.append(row)

            # Handle any remaining examples
            if example_ids[cursor:]:
                futures.append(executor.submit(fetch_examples, example_ids[cursor:]))
        result_df = pd.DataFrame(results).set_index("example_id")
        example_outputs = [
            output for future in as_completed(futures) for output in future.result()
        ]
        if example_outputs:
            example_df = pd.DataFrame(example_outputs).set_index("example_id")
            result_df = example_df.merge(result_df, left_index=True, right_index=True)

        # Flatten dict columns into dot syntax for easier access
        return pd.json_normalize(result_df.to_dict(orient="records"))

    def list_projects(
        self,
        project_ids: Optional[List[ID_TYPE]] = None,
        name: Optional[str] = None,
        name_contains: Optional[str] = None,
        reference_dataset_id: Optional[ID_TYPE] = None,
        reference_dataset_name: Optional[str] = None,
        reference_free: Optional[bool] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.TracerSession]:
        """List projects from the LangSmith API.

        Parameters
        ----------
        project_ids : Optional[List[ID_TYPE]], optional
            A list of project IDs to filter by, by default None
        name : Optional[str], optional
            The name of the project to filter by, by default None
        name_contains : Optional[str], optional
            A string to search for in the project name, by default None
        reference_dataset_id : Optional[List[ID_TYPE]], optional
            A dataset ID to filter by, by default None
        reference_dataset_name : Optional[str], optional
            The name of the reference dataset to filter by, by default None
        reference_free : Optional[bool], optional
            Whether to filter for only projects not associated with a dataset.
        limit : Optional[int], optional
            The maximum number of projects to return, by default None

        Yields:
        ------
        TracerSession
            The projects.
        """
        params: Dict[str, Any] = {
            "limit": min(limit, 100) if limit is not None else 100
        }
        if project_ids is not None:
            params["id"] = project_ids
        if name is not None:
            params["name"] = name
        if name_contains is not None:
            params["name_contains"] = name_contains
        if reference_dataset_id is not None:
            if reference_dataset_name is not None:
                raise ValueError(
                    "Only one of reference_dataset_id or"
                    " reference_dataset_name may be given"
                )
            params["reference_dataset"] = reference_dataset_id
        elif reference_dataset_name is not None:
            reference_dataset_id = self.read_dataset(
                dataset_name=reference_dataset_name
            ).id
            params["reference_dataset"] = reference_dataset_id
        if reference_free is not None:
            params["reference_free"] = reference_free
        for i, project in enumerate(
            self._get_paginated_list("/sessions", params=params)
        ):
            yield ls_schemas.TracerSession(**project, _host_url=self._host_url)
            if limit is not None and i + 1 >= limit:
                break

    @ls_utils.xor_args(("project_name", "project_id"))
    def delete_project(
        self, *, project_name: Optional[str] = None, project_id: Optional[str] = None
    ) -> None:
        """Delete a project from LangSmith.

        Parameters
        ----------
        project_name : str or None, default=None
            The name of the project to delete.
        project_id : str or None, default=None
            The ID of the project to delete.
        """
        if project_name is not None:
            project_id = str(self.read_project(project_name=project_name).id)
        elif project_id is None:
            raise ValueError("Must provide project_name or project_id")
        response = self.request_with_retries(
            "DELETE",
            f"/sessions/{_as_uuid(project_id, 'project_id')}",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def create_dataset(
        self,
        dataset_name: str,
        *,
        description: Optional[str] = None,
        data_type: ls_schemas.DataType = ls_schemas.DataType.kv,
    ) -> ls_schemas.Dataset:
        """Create a dataset in the LangSmith API.

        Parameters
        ----------
        dataset_name : str
            The name of the dataset.
        description : str or None, default=None
            The description of the dataset.
        data_type : DataType or None, default=DataType.kv
            The data type of the dataset.

        Returns:
        -------
        Dataset
            The created dataset.
        """
        dataset = ls_schemas.DatasetCreate(
            name=dataset_name,
            description=description,
            data_type=data_type,
        )
        response = self.request_with_retries(
            "POST",
            "/datasets",
            headers={**self._headers, "Content-Type": "application/json"},
            data=dataset.json(),
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.Dataset(
            **response.json(),
            _host_url=self._host_url,
            _tenant_id=self._get_optional_tenant_id(),
        )

    def has_dataset(
        self, *, dataset_name: Optional[str] = None, dataset_id: Optional[str] = None
    ) -> bool:
        """Check whether a dataset exists in your tenant.

        Parameters
        ----------
        dataset_name : str or None, default=None
            The name of the dataset to check.
        dataset_id : str or None, default=None
            The ID of the dataset to check.

        Returns:
        -------
        bool
            Whether the dataset exists.
        """
        try:
            self.read_dataset(dataset_name=dataset_name, dataset_id=dataset_id)
            return True
        except ls_utils.LangSmithNotFoundError:
            return False

    @ls_utils.xor_args(("dataset_name", "dataset_id"))
    def read_dataset(
        self,
        *,
        dataset_name: Optional[str] = None,
        dataset_id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.Dataset:
        """Read a dataset from the LangSmith API.

        Parameters
        ----------
        dataset_name : str or None, default=None
            The name of the dataset to read.
        dataset_id : UUID or None, default=None
            The ID of the dataset to read.

        Returns:
        -------
        Dataset
            The dataset.
        """
        path = "/datasets"
        params: Dict[str, Any] = {"limit": 1}
        if dataset_id is not None:
            path += f"/{_as_uuid(dataset_id, 'dataset_id')}"
        elif dataset_name is not None:
            params["name"] = dataset_name
        else:
            raise ValueError("Must provide dataset_name or dataset_id")
        response = self.request_with_retries(
            "GET",
            path,
            params=params,
        )
        result = response.json()
        if isinstance(result, list):
            if len(result) == 0:
                raise ls_utils.LangSmithNotFoundError(
                    f"Dataset {dataset_name} not found"
                )
            return ls_schemas.Dataset(
                **result[0],
                _host_url=self._host_url,
                _tenant_id=self._get_optional_tenant_id(),
            )
        return ls_schemas.Dataset(
            **result,
            _host_url=self._host_url,
            _tenant_id=self._get_optional_tenant_id(),
        )

    def diff_dataset_versions(
        self,
        dataset_id: Optional[ID_TYPE] = None,
        *,
        dataset_name: Optional[str] = None,
        from_version: Union[str, datetime.datetime],
        to_version: Union[str, datetime.datetime],
    ) -> ls_schemas.DatasetDiffInfo:
        """Get the difference between two versions of a dataset.

        Parameters
        ----------
        dataset_id : str or None, default=None
            The ID of the dataset.
        dataset_name : str or None, default=None
            The name of the dataset.
        from_version : str or datetime.datetime
            The starting version for the diff.
        to_version : str or datetime.datetime
            The ending version for the diff.

        Returns:
        -------
        DatasetDiffInfo
            The difference between the two versions of the dataset.

        Examples:
        --------
        ..code-block:: python

            # Get the difference between two tagged versions of a dataset
            from_version = "prod"
            to_version = "dev"
            diff = client.diff_dataset_versions(
                dataset_name="my-dataset",
                from_version=from_version,
                to_version=to_version,
            )
            print(diff)

            # Get the difference between two timestamped versions of a dataset

            from_version = datetime.datetime(2024, 1, 1)
            to_version = datetime.datetime(2024, 2, 1)
            diff = client.diff_dataset_versions(
                dataset_name="my-dataset",
                from_version=from_version,
                to_version=to_version,
            )
            print(diff)
        """
        if dataset_id is None:
            if dataset_name is None:
                raise ValueError("Must provide either dataset name or ID")
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        dsid = _as_uuid(dataset_id, "dataset_id")
        response = self.request_with_retries(
            "GET",
            f"/datasets/{dsid}/versions/diff",
            headers=self._headers,
            params={
                "from_version": (
                    from_version.isoformat()
                    if isinstance(from_version, datetime.datetime)
                    else from_version
                ),
                "to_version": (
                    to_version.isoformat()
                    if isinstance(to_version, datetime.datetime)
                    else to_version
                ),
            },
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.DatasetDiffInfo(**response.json())

    def read_dataset_openai_finetuning(
        self, dataset_id: Optional[str] = None, *, dataset_name: Optional[str] = None
    ) -> list:
        """Download a dataset in OpenAI Jsonl format and load it as a list of dicts.

        Parameters
        ----------
        dataset_id : str
            The ID of the dataset to download.
        dataset_name : str
            The name of the dataset to download.

        Returns:
        -------
        list
            The dataset loaded as a list of dicts.
        """
        path = "/datasets"
        if dataset_id is not None:
            pass
        elif dataset_name is not None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        else:
            raise ValueError("Must provide dataset_name or dataset_id")
        response = self.request_with_retries(
            "GET",
            f"{path}/{_as_uuid(dataset_id, 'dataset_id')}/openai_ft",
        )
        dataset = [json.loads(line) for line in response.text.strip().split("\n")]
        return dataset

    def list_datasets(
        self,
        *,
        dataset_ids: Optional[List[ID_TYPE]] = None,
        data_type: Optional[str] = None,
        dataset_name: Optional[str] = None,
        dataset_name_contains: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.Dataset]:
        """List the datasets on the LangSmith API.

        Yields:
        ------
        Dataset
            The datasets.
        """
        params: Dict[str, Any] = {
            "limit": min(limit, 100) if limit is not None else 100
        }
        if dataset_ids is not None:
            params["id"] = dataset_ids
        if data_type is not None:
            params["data_type"] = data_type
        if dataset_name is not None:
            params["name"] = dataset_name
        if dataset_name_contains is not None:
            params["name_contains"] = dataset_name_contains
        for i, dataset in enumerate(
            self._get_paginated_list("/datasets", params=params)
        ):
            yield ls_schemas.Dataset(
                **dataset,
                _host_url=self._host_url,
                _tenant_id=self._get_optional_tenant_id(),
            )
            if limit is not None and i + 1 >= limit:
                break

    @ls_utils.xor_args(("dataset_id", "dataset_name"))
    def delete_dataset(
        self,
        *,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
    ) -> None:
        """Delete a dataset from the LangSmith API.

        Parameters
        ----------
        dataset_id : UUID or None, default=None
            The ID of the dataset to delete.
        dataset_name : str or None, default=None
            The name of the dataset to delete.
        """
        if dataset_name is not None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        if dataset_id is None:
            raise ValueError("Must provide either dataset name or ID")
        response = self.request_with_retries(
            "DELETE",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def update_dataset_tag(
        self,
        *,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        as_of: datetime.datetime,
        tag: str,
    ) -> None:
        """Update the tags of a dataset.

        If the tag is already assigned to a different version of this dataset,
        the tag will be moved to the new version. The as_of parameter is used to
        determine which version of the dataset to apply the new tags to.
        It must be an exact version of the dataset to succeed. You can
        use the read_dataset_version method to find the exact version
        to apply the tags to.

        Parameters
        ----------
        dataset_id : UUID
            The ID of the dataset to update.
        as_of : datetime.datetime
            The timestamp of the dataset to apply the new tags to.
        tag : str
            The new tag to apply to the dataset.

        Examples:
        --------
        .. code-block:: python

            dataset_name = "my-dataset"
            # Get the version of a dataset <= a given timestamp
            dataset_version = client.read_dataset_version(
                dataset_name=dataset_name, as_of=datetime.datetime(2024, 1, 1)
            )
            # Assign that version a new tag
            client.update_dataset_tags(
                dataset_name="my-dataset",
                as_of=dataset_version.as_of,
                tag="prod",
            )
        """
        if dataset_name is not None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        if dataset_id is None:
            raise ValueError("Must provide either dataset name or ID")
        response = self.request_with_retries(
            "PUT",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/tags",
            headers=self._headers,
            json={
                "as_of": as_of.isoformat(),
                "tag": tag,
            },
        )
        ls_utils.raise_for_status_with_text(response)

    def list_dataset_versions(
        self,
        *,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        search: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.DatasetVersion]:
        """List dataset versions.

        Args:
            dataset_id (Optional[ID_TYPE]): The ID of the dataset.
            dataset_name (Optional[str]): The name of the dataset.
            search (Optional[str]): The search query.
            limit (Optional[int]): The maximum number of versions to return.

        Returns:
            Iterator[ls_schemas.DatasetVersion]: An iterator of dataset versions.
        """
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        params = {
            "search": search,
            "limit": min(limit, 100) if limit is not None else 100,
        }
        for i, version in enumerate(
            self._get_paginated_list(
                f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/versions",
                params=params,
            )
        ):
            yield ls_schemas.DatasetVersion(**version)
            if limit is not None and i + 1 >= limit:
                break

    def read_dataset_version(
        self,
        *,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        as_of: Optional[datetime.datetime] = None,
        tag: Optional[str] = None,
    ) -> ls_schemas.DatasetVersion:
        """Get dataset version by as_of or exact tag.

        Ues this to resolve the nearest version to a given timestamp or for a given tag.

        Args:
            dataset_id (Optional[ID_TYPE]): The ID of the dataset.
            dataset_name (Optional[str]): The name of the dataset.
            as_of (Optional[datetime.datetime]): The timestamp of the dataset
                to retrieve.
            tag (Optional[str]): The tag of the dataset to retrieve.

        Returns:
            ls_schemas.DatasetVersion: The dataset version.


        Examples:
        --------
        .. code-block:: python

            # Get the latest version of a dataset
            client.read_dataset_version(dataset_name="my-dataset", tag="latest")

            # Get the version of a dataset <= a given timestamp
            client.read_dataset_version(
                dataset_name="my-dataset",
                as_of=datetime.datetime(2024, 1, 1),
            )


            # Get the version of a dataset with a specific tag
            client.read_dataset_version(dataset_name="my-dataset", tag="prod")
        """
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        if (as_of and tag) or (as_of is None and tag is None):
            raise ValueError("Exactly one of as_of and tag must be specified.")
        response = self.request_with_retries(
            "GET",
            f"/datasets/{_as_uuid(dataset_id, 'dataset_id')}/version",
            params={"as_of": as_of, "tag": tag},
        )
        return ls_schemas.DatasetVersion(**response.json())

    def clone_public_dataset(
        self,
        token_or_url: str,
        *,
        source_api_url: Optional[str] = None,
        dataset_name: Optional[str] = None,
    ) -> None:
        """Clone a public dataset to your own langsmith tenant.

        This operation is idempotent. If you already have a dataset with the given name,
        this function will do nothing.

        Args:
            token_or_url (str): The token of the public dataset to clone.
            source_api_url: The URL of the langsmith server where the data is hosted.
                Defaults to the API URL of your current client.
            dataset_name (str): The name of the dataset to create in your tenant.
                Defaults to the name of the public dataset.

        """
        source_api_url = source_api_url or self.api_url
        source_api_url, token_uuid = _parse_token_or_url(token_or_url, source_api_url)
        source_client = Client(
            # Placeholder API key not needed anymore in most cases, but
            # some private deployments may have API key-based rate limiting
            # that would cause this to fail if we provide no value.
            api_url=source_api_url,
            api_key="placeholder",
        )
        ds = source_client.read_shared_dataset(token_uuid)
        dataset_name = dataset_name or ds.name
        if self.has_dataset(dataset_name=dataset_name):
            logger.info(
                f"Dataset {dataset_name} already exists in your tenant. Skipping."
            )
            return
        try:
            # Fetch examples first
            examples = list(source_client.list_shared_examples(token_uuid))
            dataset = self.create_dataset(
                dataset_name=dataset_name,
                description=ds.description,
                data_type=ds.data_type or ls_schemas.DataType.kv,
            )
            try:
                self.create_examples(
                    inputs=[e.inputs for e in examples],
                    outputs=[e.outputs for e in examples],
                    dataset_id=dataset.id,
                )
            except BaseException as e:
                # Let's not do automatic clean up for now in case there might be
                # some other reasons why create_examples fails (i.e., not network issue
                # or keyboard interrupt).
                # The risk is that this is an existing dataset that has valid examples
                # populated from another source so we don't want to delete it.
                logger.error(
                    f"An error occurred while creating dataset {dataset_name}. "
                    "You should delete it manually."
                )
                raise e
        finally:
            del source_client

    def _get_data_type(self, dataset_id: ID_TYPE) -> ls_schemas.DataType:
        dataset = self.read_dataset(dataset_id=dataset_id)
        return dataset.data_type

    @ls_utils.xor_args(("dataset_id", "dataset_name"))
    def create_llm_example(
        self,
        prompt: str,
        generation: Optional[str] = None,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
    ) -> ls_schemas.Example:
        """Add an example (row) to an LLM-type dataset."""
        return self.create_example(
            inputs={"input": prompt},
            outputs={"output": generation},
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            created_at=created_at,
        )

    @ls_utils.xor_args(("dataset_id", "dataset_name"))
    def create_chat_example(
        self,
        messages: List[Union[Mapping[str, Any], ls_schemas.BaseMessageLike]],
        generations: Optional[
            Union[Mapping[str, Any], ls_schemas.BaseMessageLike]
        ] = None,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
    ) -> ls_schemas.Example:
        """Add an example (row) to a Chat-type dataset."""
        final_input = []
        for message in messages:
            if ls_utils.is_base_message_like(message):
                final_input.append(
                    ls_utils.convert_langchain_message(
                        cast(ls_schemas.BaseMessageLike, message)
                    )
                )
            else:
                final_input.append(cast(dict, message))
        final_generations = None
        if generations is not None:
            if ls_utils.is_base_message_like(generations):
                final_generations = ls_utils.convert_langchain_message(
                    cast(ls_schemas.BaseMessageLike, generations)
                )
            else:
                final_generations = cast(dict, generations)
        return self.create_example(
            inputs={"input": final_input},
            outputs=(
                {"output": final_generations} if final_generations is not None else None
            ),
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            created_at=created_at,
        )

    def create_example_from_run(
        self,
        run: ls_schemas.Run,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
    ) -> ls_schemas.Example:
        """Add an example (row) to an LLM-type dataset."""
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
            dataset_name = None  # Nested call expects only 1 defined
        dataset_type = self._get_data_type_cached(dataset_id)
        if dataset_type == ls_schemas.DataType.llm:
            if run.run_type != "llm":
                raise ValueError(
                    f"Run type {run.run_type} is not supported"
                    " for dataset of type 'LLM'"
                )
            try:
                prompt = ls_utils.get_prompt_from_inputs(run.inputs)
            except ValueError:
                raise ValueError(
                    "Error converting LLM run inputs to prompt for run"
                    f" {run.id} with inputs {run.inputs}"
                )
            inputs: Dict[str, Any] = {"input": prompt}
            if not run.outputs:
                outputs: Optional[Dict[str, Any]] = None
            else:
                try:
                    generation = ls_utils.get_llm_generation_from_outputs(run.outputs)
                except ValueError:
                    raise ValueError(
                        "Error converting LLM run outputs to generation for run"
                        f" {run.id} with outputs {run.outputs}"
                    )
                outputs = {"output": generation}
        elif dataset_type == ls_schemas.DataType.chat:
            if run.run_type != "llm":
                raise ValueError(
                    f"Run type {run.run_type} is not supported"
                    " for dataset of type 'chat'"
                )
            try:
                inputs = {"input": ls_utils.get_messages_from_inputs(run.inputs)}
            except ValueError:
                raise ValueError(
                    "Error converting LLM run inputs to chat messages for run"
                    f" {run.id} with inputs {run.inputs}"
                )
            if not run.outputs:
                outputs = None
            else:
                try:
                    outputs = {
                        "output": ls_utils.get_message_generation_from_outputs(
                            run.outputs
                        )
                    }
                except ValueError:
                    raise ValueError(
                        "Error converting LLM run outputs to chat generations"
                        f" for run {run.id} with outputs {run.outputs}"
                    )
        elif dataset_type == ls_schemas.DataType.kv:
            # Anything goes
            inputs = run.inputs
            outputs = run.outputs

        else:
            raise ValueError(f"Dataset type {dataset_type} not recognized.")
        return self.create_example(
            inputs=inputs,
            outputs=outputs,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            created_at=created_at,
        )

    def create_examples(
        self,
        *,
        inputs: Sequence[Mapping[str, Any]],
        outputs: Optional[Sequence[Optional[Mapping[str, Any]]]] = None,
        metadata: Optional[Sequence[Optional[Mapping[str, Any]]]] = None,
        splits: Optional[Sequence[Optional[str | List[str]]]] = None,
        source_run_ids: Optional[Sequence[Optional[ID_TYPE]]] = None,
        ids: Optional[Sequence[Optional[ID_TYPE]]] = None,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Create examples in a dataset.

        Parameters
        ----------
        inputs : Sequence[Mapping[str, Any]]
            The input values for the examples.
        outputs : Optional[Sequence[Optional[Mapping[str, Any]]]], default=None
            The output values for the examples.
        metadata : Optional[Sequence[Optional[Mapping[str, Any]]]], default=None
            The metadata for the examples.
        split :  Optional[Sequence[Optional[str | List[str]]]], default=None
            The splits for the examples, which are divisions
            of your dataset such as 'train', 'test', or 'validation'.
        source_run_ids : Optional[Sequence[Optional[ID_TYPE]]], default=None
                The IDs of the source runs associated with the examples.
        ids : Optional[Sequence[ID_TYPE]], default=None
            The IDs of the examples.
        dataset_id : Optional[ID_TYPE], default=None
            The ID of the dataset to create the examples in.
        dataset_name : Optional[str], default=None
            The name of the dataset to create the examples in.

        Returns:
        -------
        None

        Raises:
        ------
        ValueError
            If both `dataset_id` and `dataset_name` are `None`.
        """
        if dataset_id is None and dataset_name is None:
            raise ValueError("Either dataset_id or dataset_name must be provided.")

        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
        examples = [
            {
                "inputs": in_,
                "outputs": out_,
                "dataset_id": dataset_id,
                "metadata": metadata_,
                "split": split_,
                "id": id_ or str(uuid.uuid4()),
                "source_run_id": source_run_id_,
            }
            for in_, out_, metadata_, split_, id_, source_run_id_ in zip(
                inputs,
                outputs or [None] * len(inputs),
                metadata or [None] * len(inputs),
                splits or [None] * len(inputs),
                ids or [None] * len(inputs),
                source_run_ids or [None] * len(inputs),
            )
        ]

        response = self.request_with_retries(
            "POST",
            "/examples/bulk",
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json(examples),
        )
        ls_utils.raise_for_status_with_text(response)

    @ls_utils.xor_args(("dataset_id", "dataset_name"))
    def create_example(
        self,
        inputs: Mapping[str, Any],
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
        outputs: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        split: Optional[str | List[str]] = None,
        example_id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.Example:
        """Create a dataset example in the LangSmith API.

        Examples are rows in a dataset, containing the inputs
        and expected outputs (or other reference information)
        for a model or chain.

        Args:
            inputs : Mapping[str, Any]
                The input values for the example.
            dataset_id : UUID or None, default=None
                The ID of the dataset to create the example in.
            dataset_name : str or None, default=None
                The name of the dataset to create the example in.
            created_at : datetime or None, default=None
                The creation timestamp of the example.
            outputs : Mapping[str, Any] or None, default=None
                The output values for the example.
            metadata : Mapping[str, Any] or None, default=None
                The metadata for the example.
            split : str or List[str] or None, default=None
                The splits for the example, which are divisions
                of your dataset such as 'train', 'test', or 'validation'.
            exemple_id : UUID or None, default=None
                The ID of the example to create. If not provided, a new
                example will be created.

        Returns:
            Example: The created example.
        """
        if dataset_id is None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id

        data = {
            "inputs": inputs,
            "outputs": outputs,
            "dataset_id": dataset_id,
            "metadata": metadata,
            "split": split,
        }
        if created_at:
            data["created_at"] = created_at.isoformat()
        data["id"] = example_id or str(uuid.uuid4())
        response = self.request_with_retries(
            "POST",
            "/examples",
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json({k: v for k, v in data.items() if v is not None}),
        )
        ls_utils.raise_for_status_with_text(response)
        result = response.json()
        return ls_schemas.Example(
            **result,
            _host_url=self._host_url,
            _tenant_id=self._get_optional_tenant_id(),
        )

    def read_example(
        self, example_id: ID_TYPE, *, as_of: Optional[datetime.datetime] = None
    ) -> ls_schemas.Example:
        """Read an example from the LangSmith API.

        Args:
            example_id (UUID): The ID of the example to read.

        Returns:
            Example: The example.
        """
        response = self.request_with_retries(
            "GET",
            f"/examples/{_as_uuid(example_id, 'example_id')}",
            params={
                "as_of": as_of.isoformat() if as_of else None,
            },
        )
        return ls_schemas.Example(
            **response.json(),
            _host_url=self._host_url,
            _tenant_id=self._get_optional_tenant_id(),
        )

    def list_examples(
        self,
        dataset_id: Optional[ID_TYPE] = None,
        dataset_name: Optional[str] = None,
        example_ids: Optional[Sequence[ID_TYPE]] = None,
        as_of: Optional[Union[datetime.datetime, str]] = None,
        splits: Optional[Sequence[str]] = None,
        inline_s3_urls: bool = True,
        limit: Optional[int] = None,
        metadata: Optional[dict] = None,
        **kwargs: Any,
    ) -> Iterator[ls_schemas.Example]:
        """Retrieve the example rows of the specified dataset.

        Args:
            dataset_id (UUID, optional): The ID of the dataset to filter by.
                Defaults to None.
            dataset_name (str, optional): The name of the dataset to filter by.
                Defaults to None.
            example_ids (List[UUID], optional): The IDs of the examples to filter by.
                Defaults to None.
            as_of (datetime, str, or optional): The dataset version tag OR
                timestamp to retrieve the examples as of.
                Response examples will only be those that were present at the time
                of the tagged (or timestamped) version.
            splits (List[str], optional): A list of dataset splits, which are
                divisions of your dataset such as 'train', 'test', or 'validation'.
                Returns examples only from the specified splits.
            inline_s3_urls (bool, optional): Whether to inline S3 URLs.
                Defaults to True.
            limit (int, optional): The maximum number of examples to return.

        Yields:
            Example: The examples.
        """
        params: Dict[str, Any] = {
            **kwargs,
            "id": example_ids,
            "as_of": (
                as_of.isoformat() if isinstance(as_of, datetime.datetime) else as_of
            ),
            "splits": splits,
            "inline_s3_urls": inline_s3_urls,
            "limit": min(limit, 100) if limit is not None else 100,
        }
        if metadata is not None:
            params["metadata"] = _dumps_json(metadata)
        if dataset_id is not None:
            params["dataset"] = dataset_id
        elif dataset_name is not None:
            dataset_id = self.read_dataset(dataset_name=dataset_name).id
            params["dataset"] = dataset_id
        else:
            pass
        for i, example in enumerate(
            self._get_paginated_list("/examples", params=params)
        ):
            yield ls_schemas.Example(
                **example,
                _host_url=self._host_url,
                _tenant_id=self._get_optional_tenant_id(),
            )
            if limit is not None and i + 1 >= limit:
                break

    def update_example(
        self,
        example_id: ID_TYPE,
        *,
        inputs: Optional[Dict[str, Any]] = None,
        outputs: Optional[Mapping[str, Any]] = None,
        metadata: Optional[Dict] = None,
        split: Optional[str | List[str]] = None,
        dataset_id: Optional[ID_TYPE] = None,
    ) -> Dict[str, Any]:
        """Update a specific example.

        Parameters
        ----------
        example_id : str or UUID
            The ID of the example to update.
        inputs : Dict[str, Any] or None, default=None
            The input values to update.
        outputs : Mapping[str, Any] or None, default=None
            The output values to update.
        metadata : Dict or None, default=None
            The metadata to update.
        split : str or List[str] or None, default=None
            The dataset split to update, such as
            'train', 'test', or 'validation'.
        dataset_id : UUID or None, default=None
            The ID of the dataset to update.

        Returns:
        -------
        Dict[str, Any]
            The updated example.
        """
        example = dict(
            inputs=inputs,
            outputs=outputs,
            dataset_id=dataset_id,
            metadata=metadata,
            split=split,
        )
        response = self.request_with_retries(
            "PATCH",
            f"/examples/{_as_uuid(example_id, 'example_id')}",
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json({k: v for k, v in example.items() if v is not None}),
        )
        ls_utils.raise_for_status_with_text(response)
        return response.json()

    def delete_example(self, example_id: ID_TYPE) -> None:
        """Delete an example by ID.

        Parameters
        ----------
        example_id : str or UUID
            The ID of the example to delete.
        """
        response = self.request_with_retries(
            "DELETE",
            f"/examples/{_as_uuid(example_id, 'example_id')}",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def _resolve_run_id(
        self,
        run: Union[ls_schemas.Run, ls_schemas.RunBase, str, uuid.UUID],
        load_child_runs: bool,
    ) -> ls_schemas.Run:
        """Resolve the run ID.

        Parameters
        ----------
        run : Run or RunBase or str or UUID
            The run to resolve.
        load_child_runs : bool
            Whether to load child runs.

        Returns:
        -------
        Run
            The resolved run.

        Raises:
        ------
        TypeError
            If the run type is invalid.
        """
        if isinstance(run, (str, uuid.UUID)):
            run_ = self.read_run(run, load_child_runs=load_child_runs)
        else:
            run_ = cast(ls_schemas.Run, run)
        return run_

    def _resolve_example_id(
        self,
        example: Union[ls_schemas.Example, str, uuid.UUID, dict, None],
        run: ls_schemas.Run,
    ) -> Optional[ls_schemas.Example]:
        """Resolve the example ID.

        Parameters
        ----------
        example : Example or str or UUID or dict or None
            The example to resolve.
        run : Run
            The run associated with the example.

        Returns:
        -------
        Example or None
            The resolved example.
        """
        if isinstance(example, (str, uuid.UUID)):
            reference_example_ = self.read_example(example)
        elif isinstance(example, ls_schemas.Example):
            reference_example_ = example
        elif isinstance(example, dict):
            reference_example_ = ls_schemas.Example(
                **example,
                _host_url=self._host_url,
                _tenant_id=self._get_optional_tenant_id(),
            )
        elif run.reference_example_id is not None:
            reference_example_ = self.read_example(run.reference_example_id)
        else:
            reference_example_ = None
        return reference_example_

    def _select_eval_results(
        self,
        results: Union[ls_evaluator.EvaluationResult, ls_evaluator.EvaluationResults],
        *,
        fn_name: Optional[str] = None,
    ) -> List[ls_evaluator.EvaluationResult]:
        from langsmith.evaluation import evaluator as ls_evaluator  # noqa: F811

        if isinstance(results, ls_evaluator.EvaluationResult):
            results_ = [results]
        elif isinstance(results, dict):
            if "results" in results:
                results_ = cast(List[ls_evaluator.EvaluationResult], results["results"])
            else:
                results_ = [
                    ls_evaluator.EvaluationResult(**{"key": fn_name, **results})  # type: ignore[arg-type]
                ]
        else:
            raise TypeError(
                f"Invalid evaluation result type {type(results)}."
                " Expected EvaluationResult or EvaluationResults."
            )
        return results_

    def evaluate_run(
        self,
        run: Union[ls_schemas.Run, ls_schemas.RunBase, str, uuid.UUID],
        evaluator: ls_evaluator.RunEvaluator,
        *,
        source_info: Optional[Dict[str, Any]] = None,
        reference_example: Optional[
            Union[ls_schemas.Example, str, dict, uuid.UUID]
        ] = None,
        load_child_runs: bool = False,
    ) -> ls_evaluator.EvaluationResult:
        """Evaluate a run.

        Parameters
        ----------
        run : Run or RunBase or str or UUID
            The run to evaluate.
        evaluator : RunEvaluator
            The evaluator to use.
        source_info : Dict[str, Any] or None, default=None
            Additional information about the source of the evaluation to log
            as feedback metadata.
        reference_example : Example or str or dict or UUID or None, default=None
            The example to use as a reference for the evaluation.
            If not provided, the run's reference example will be used.
        load_child_runs : bool, default=False
            Whether to load child runs when resolving the run ID.

        Returns:
        -------
        Feedback
            The feedback object created by the evaluation.
        """
        run_ = self._resolve_run_id(run, load_child_runs=load_child_runs)
        reference_example_ = self._resolve_example_id(reference_example, run_)
        evaluator_response = evaluator.evaluate_run(
            run_,
            example=reference_example_,
        )
        results = self._log_evaluation_feedback(
            evaluator_response,
            run_,
            source_info=source_info,
        )
        # TODO: Return all results
        return results[0]

    def _log_evaluation_feedback(
        self,
        evaluator_response: Union[
            ls_evaluator.EvaluationResult, ls_evaluator.EvaluationResults
        ],
        run: Optional[ls_schemas.Run] = None,
        source_info: Optional[Dict[str, Any]] = None,
        project_id: Optional[ID_TYPE] = None,
    ) -> List[ls_evaluator.EvaluationResult]:
        results = self._select_eval_results(evaluator_response)
        for res in results:
            source_info_ = source_info or {}
            if res.evaluator_info:
                source_info_ = {**res.evaluator_info, **source_info_}
            run_id_ = None
            if res.target_run_id:
                run_id_ = res.target_run_id
            elif run is not None:
                run_id_ = run.id
            self.create_feedback(
                run_id_,
                res.key,
                score=res.score,
                value=res.value,
                comment=res.comment,
                correction=res.correction,
                source_info=source_info_,
                source_run_id=res.source_run_id,
                feedback_config=cast(
                    Optional[ls_schemas.FeedbackConfig], res.feedback_config
                ),
                feedback_source_type=ls_schemas.FeedbackSourceType.MODEL,
                project_id=project_id,
            )
        return results

    async def aevaluate_run(
        self,
        run: Union[ls_schemas.Run, str, uuid.UUID],
        evaluator: ls_evaluator.RunEvaluator,
        *,
        source_info: Optional[Dict[str, Any]] = None,
        reference_example: Optional[
            Union[ls_schemas.Example, str, dict, uuid.UUID]
        ] = None,
        load_child_runs: bool = False,
    ) -> ls_evaluator.EvaluationResult:
        """Evaluate a run asynchronously.

        Parameters
        ----------
        run : Run or str or UUID
            The run to evaluate.
        evaluator : RunEvaluator
            The evaluator to use.
        source_info : Dict[str, Any] or None, default=None
            Additional information about the source of the evaluation to log
            as feedback metadata.
        reference_example : Optional Example or UUID, default=None
            The example to use as a reference for the evaluation.
            If not provided, the run's reference example will be used.
        load_child_runs : bool, default=False
            Whether to load child runs when resolving the run ID.

        Returns:
        -------
        EvaluationResult
            The evaluation result object created by the evaluation.
        """
        run_ = self._resolve_run_id(run, load_child_runs=load_child_runs)
        reference_example_ = self._resolve_example_id(reference_example, run_)
        evaluator_response = await evaluator.aevaluate_run(
            run_,
            example=reference_example_,
        )
        # TODO: Return all results and use async API
        results = self._log_evaluation_feedback(
            evaluator_response,
            run_,
            source_info=source_info,
        )
        return results[0]

    def create_feedback(
        self,
        run_id: Optional[ID_TYPE],
        key: str,
        *,
        score: Union[float, int, bool, None] = None,
        value: Union[float, int, bool, str, dict, None] = None,
        correction: Union[dict, None] = None,
        comment: Union[str, None] = None,
        source_info: Optional[Dict[str, Any]] = None,
        feedback_source_type: Union[
            ls_schemas.FeedbackSourceType, str
        ] = ls_schemas.FeedbackSourceType.API,
        source_run_id: Optional[ID_TYPE] = None,
        feedback_id: Optional[ID_TYPE] = None,
        feedback_config: Optional[ls_schemas.FeedbackConfig] = None,
        stop_after_attempt: int = 10,
        project_id: Optional[ID_TYPE] = None,
        comparative_experiment_id: Optional[ID_TYPE] = None,
        feedback_group_id: Optional[ID_TYPE] = None,
        **kwargs: Any,
    ) -> ls_schemas.Feedback:
        """Create a feedback in the LangSmith API.

        Parameters
        ----------
        run_id : str or UUID
            The ID of the run to provide feedback for. Either the run_id OR
            the project_id must be provided.
        key : str
            The name of the metric or 'aspect' this feedback is about.
        score : float or int or bool or None, default=None
            The score to rate this run on the metric or aspect.
        value : float or int or bool or str or dict or None, default=None
            The display value or non-numeric value for this feedback.
        correction : dict or None, default=None
            The proper ground truth for this run.
        comment : str or None, default=None
            A comment about this feedback, such as a justification for the score or
            chain-of-thought trajectory for an LLM judge.
        source_info : Dict[str, Any] or None, default=None
            Information about the source of this feedback.
        feedback_source_type : FeedbackSourceType or str, default=FeedbackSourceType.API
            The type of feedback source, such as model (for model-generated feedback)
                or API.
        source_run_id : str or UUID or None, default=None,
            The ID of the run that generated this feedback, if a "model" type.
        feedback_id : str or UUID or None, default=None
            The ID of the feedback to create. If not provided, a random UUID will be
            generated.
        feedback_config: FeedbackConfig or None, default=None,
            The configuration specifying how to interpret feedback with this key.
            Examples include continuous (with min/max bounds), categorical,
            or freeform.
        stop_after_attempt : int, default=10
            The number of times to retry the request before giving up.
        project_id : str or UUID
            The ID of the project_id to provide feedback on. One - and only one - of
            this and run_id must be provided.
        comparative_experiment_id : str or UUID
            If this feedback was logged as a part of a comparative experiment, this
            associates the feedback with that experiment.
        feedback_group_id : str or UUID
            When logging preferences, ranking runs, or other comparative feedback,
            this is used to group feedback together.
        """
        if run_id is None and project_id is None:
            raise ValueError("One of run_id and project_id must be provided")
        if run_id is not None and project_id is not None:
            raise ValueError("Only one of run_id and project_id must be provided")
        if kwargs:
            warnings.warn(
                "The following arguments are no longer used in the create_feedback"
                f" endpoint: {sorted(kwargs)}",
                DeprecationWarning,
            )
        if not isinstance(feedback_source_type, ls_schemas.FeedbackSourceType):
            feedback_source_type = ls_schemas.FeedbackSourceType(feedback_source_type)
        if feedback_source_type == ls_schemas.FeedbackSourceType.API:
            feedback_source: ls_schemas.FeedbackSourceBase = (
                ls_schemas.APIFeedbackSource(metadata=source_info)
            )
        elif feedback_source_type == ls_schemas.FeedbackSourceType.MODEL:
            feedback_source = ls_schemas.ModelFeedbackSource(metadata=source_info)
        else:
            raise ValueError(f"Unknown feedback source type {feedback_source_type}")
        feedback_source.metadata = (
            feedback_source.metadata if feedback_source.metadata is not None else {}
        )
        if source_run_id is not None and "__run" not in feedback_source.metadata:
            feedback_source.metadata["__run"] = {"run_id": str(source_run_id)}
        if feedback_source.metadata and "__run" in feedback_source.metadata:
            # Validate that the linked run ID is a valid UUID
            # Run info may be a base model or dict.
            _run_meta: Union[dict, Any] = feedback_source.metadata["__run"]
            if hasattr(_run_meta, "dict") and callable(_run_meta):
                _run_meta = _run_meta.dict()
            if "run_id" in _run_meta:
                _run_meta["run_id"] = str(
                    _as_uuid(
                        feedback_source.metadata["__run"]["run_id"],
                        "feedback_source.metadata['__run']['run_id']",
                    )
                )
            feedback_source.metadata["__run"] = _run_meta
        feedback = ls_schemas.FeedbackCreate(
            id=_ensure_uuid(feedback_id),
            run_id=_ensure_uuid(run_id),
            key=key,
            score=score,
            value=value,
            correction=correction,
            comment=comment,
            feedback_source=feedback_source,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            modified_at=datetime.datetime.now(datetime.timezone.utc),
            feedback_config=feedback_config,
            session_id=_ensure_uuid(project_id, accept_null=True),
            comparative_experiment_id=_ensure_uuid(
                comparative_experiment_id, accept_null=True
            ),
            feedback_group_id=_ensure_uuid(feedback_group_id, accept_null=True),
        )
        feedback_block = _dumps_json(feedback.dict(exclude_none=True))
        self.request_with_retries(
            "POST",
            "/feedback",
            request_kwargs={
                "data": feedback_block,
            },
            stop_after_attempt=stop_after_attempt,
            retry_on=(ls_utils.LangSmithNotFoundError,),
        )
        return ls_schemas.Feedback(**feedback.dict())

    def update_feedback(
        self,
        feedback_id: ID_TYPE,
        *,
        score: Union[float, int, bool, None] = None,
        value: Union[float, int, bool, str, dict, None] = None,
        correction: Union[dict, None] = None,
        comment: Union[str, None] = None,
    ) -> None:
        """Update a feedback in the LangSmith API.

        Parameters
        ----------
        feedback_id : str or UUID
            The ID of the feedback to update.
        score : float or int or bool or None, default=None
            The score to update the feedback with.
        value : float or int or bool or str or dict or None, default=None
            The value to update the feedback with.
        correction : dict or None, default=None
            The correction to update the feedback with.
        comment : str or None, default=None
            The comment to update the feedback with.
        """
        feedback_update: Dict[str, Any] = {}
        if score is not None:
            feedback_update["score"] = score
        if value is not None:
            feedback_update["value"] = value
        if correction is not None:
            feedback_update["correction"] = correction
        if comment is not None:
            feedback_update["comment"] = comment
        response = self.request_with_retries(
            "PATCH",
            f"/feedback/{_as_uuid(feedback_id, 'feedback_id')}",
            headers={**self._headers, "Content-Type": "application/json"},
            data=_dumps_json(feedback_update),
        )
        ls_utils.raise_for_status_with_text(response)

    def read_feedback(self, feedback_id: ID_TYPE) -> ls_schemas.Feedback:
        """Read a feedback from the LangSmith API.

        Parameters
        ----------
        feedback_id : str or UUID
            The ID of the feedback to read.

        Returns:
        -------
        Feedback
            The feedback.
        """
        response = self.request_with_retries(
            "GET",
            f"/feedback/{_as_uuid(feedback_id, 'feedback_id')}",
        )
        return ls_schemas.Feedback(**response.json())

    def list_feedback(
        self,
        *,
        run_ids: Optional[Sequence[ID_TYPE]] = None,
        feedback_key: Optional[Sequence[str]] = None,
        feedback_source_type: Optional[Sequence[ls_schemas.FeedbackSourceType]] = None,
        limit: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator[ls_schemas.Feedback]:
        """List the feedback objects on the LangSmith API.

        Parameters
        ----------
        run_ids : List[str or UUID] or None, default=None
            The IDs of the runs to filter by.
        feedback_key: List[str] or None, default=None
            The feedback key(s) to filter by. Example: 'correctness'
            The query performs a union of all feedback keys.
        feedback_source_type: List[FeedbackSourceType] or None, default=None
            The type of feedback source, such as model
            (for model-generated feedback) or API.
        limit : int or None, default=None
        **kwargs : Any
            Additional keyword arguments.

        Yields:
        ------
        Feedback
            The feedback objects.
        """
        params: dict = {
            "run": run_ids,
            "limit": min(limit, 100) if limit is not None else 100,
            **kwargs,
        }
        if feedback_key is not None:
            params["key"] = feedback_key
        if feedback_source_type is not None:
            params["source"] = feedback_source_type
        for i, feedback in enumerate(
            self._get_paginated_list("/feedback", params=params)
        ):
            yield ls_schemas.Feedback(**feedback)
            if limit is not None and i + 1 >= limit:
                break

    def delete_feedback(self, feedback_id: ID_TYPE) -> None:
        """Delete a feedback by ID.

        Parameters
        ----------
        feedback_id : str or UUID
            The ID of the feedback to delete.
        """
        response = self.request_with_retries(
            "DELETE",
            f"/feedback/{_as_uuid(feedback_id, 'feedback_id')}",
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def create_feedback_from_token(
        self,
        token_or_url: Union[str, uuid.UUID],
        score: Union[float, int, bool, None] = None,
        *,
        value: Union[float, int, bool, str, dict, None] = None,
        correction: Union[dict, None] = None,
        comment: Union[str, None] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Create feedback from a presigned token or URL.

        Args:
            token_or_url (Union[str, uuid.UUID]): The token or URL from which to create
                 feedback.
            score (Union[float, int, bool, None], optional): The score of the feedback.
                Defaults to None.
            value (Union[float, int, bool, str, dict, None], optional): The value of the
                feedback. Defaults to None.
            correction (Union[dict, None], optional): The correction of the feedback.
                Defaults to None.
            comment (Union[str, None], optional): The comment of the feedback. Defaults
                to None.
            metadata (Optional[dict], optional): Additional metadata for the feedback.
                Defaults to None.

        Raises:
            ValueError: If the source API URL is invalid.

        Returns:
            None: This method does not return anything.
        """
        source_api_url, token_uuid = _parse_token_or_url(
            token_or_url, self.api_url, num_parts=1
        )
        if source_api_url != self.api_url:
            raise ValueError(f"Invalid source API URL. {source_api_url}")
        response = self.request_with_retries(
            "POST",
            f"/feedback/tokens/{_as_uuid(token_uuid)}",
            data=_dumps_json(
                {
                    "score": score,
                    "value": value,
                    "correction": correction,
                    "comment": comment,
                    "metadata": metadata,
                    # TODO: Add ID once the API supports it.
                }
            ),
            headers=self._headers,
        )
        ls_utils.raise_for_status_with_text(response)

    def create_presigned_feedback_token(
        self,
        run_id: ID_TYPE,
        feedback_key: str,
        *,
        expiration: Optional[datetime.datetime | datetime.timedelta] = None,
        feedback_config: Optional[ls_schemas.FeedbackConfig] = None,
        feedback_id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.FeedbackIngestToken:
        """Create a pre-signed URL to send feedback data to.

        This is useful for giving browser-based clients a way to upload
        feedback data directly to LangSmith without accessing the
        API key.

        Args:
            run_id:
            feedback_key:
            expiration: The expiration time of the pre-signed URL.
                Either a datetime or a timedelta offset from now.
                Default to 3 hours.
            feedback_config: FeedbackConfig or None.
                If creating a feedback_key for the first time,
                this defines how the metric should be interpreted,
                such as a continuous score (w/ optional bounds),
                or distribution over categorical values.
            feedback_id: The ID of the feedback to create. If not provided, a new
                feedback will be created.

        Returns:
            The pre-signed URL for uploading feedback data.
        """
        body: Dict[str, Any] = {
            "run_id": run_id,
            "feedback_key": feedback_key,
            "feedback_config": feedback_config,
            "id": feedback_id or str(uuid.uuid4()),
        }
        if expiration is None:
            body["expires_in"] = ls_schemas.TimeDeltaInput(
                days=0,
                hours=3,
                minutes=0,
            )
        elif isinstance(expiration, datetime.datetime):
            body["expires_at"] = expiration.isoformat()
        elif isinstance(expiration, datetime.timedelta):
            body["expires_in"] = ls_schemas.TimeDeltaInput(
                days=expiration.days,
                hours=expiration.seconds // 3600,
                minutes=(expiration.seconds // 60) % 60,
            )
        else:
            raise ValueError(f"Unknown expiration type: {type(expiration)}")

        response = self.request_with_retries(
            "POST",
            "/feedback/tokens",
            data=_dumps_json(body),
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.FeedbackIngestToken(**response.json())

    def create_presigned_feedback_tokens(
        self,
        run_id: ID_TYPE,
        feedback_keys: Sequence[str],
        *,
        expiration: Optional[datetime.datetime | datetime.timedelta] = None,
        feedback_configs: Optional[
            Sequence[Optional[ls_schemas.FeedbackConfig]]
        ] = None,
    ) -> Sequence[ls_schemas.FeedbackIngestToken]:
        """Create a pre-signed URL to send feedback data to.

        This is useful for giving browser-based clients a way to upload
        feedback data directly to LangSmith without accessing the
        API key.

        Args:
            run_id:
            feedback_key:
            expiration: The expiration time of the pre-signed URL.
                Either a datetime or a timedelta offset from now.
                Default to 3 hours.
            feedback_config: FeedbackConfig or None.
                If creating a feedback_key for the first time,
                this defines how the metric should be interpreted,
                such as a continuous score (w/ optional bounds),
                or distribution over categorical values.

        Returns:
            The pre-signed URL for uploading feedback data.
        """
        # validate
        if feedback_configs is not None and len(feedback_keys) != len(feedback_configs):
            raise ValueError(
                "The length of feedback_keys and feedback_configs must be the same."
            )
        if not feedback_configs:
            feedback_configs = [None] * len(feedback_keys)
        # build expiry option
        expires_in, expires_at = None, None
        if expiration is None:
            expires_in = ls_schemas.TimeDeltaInput(
                days=0,
                hours=3,
                minutes=0,
            )
        elif isinstance(expiration, datetime.datetime):
            expires_at = expiration.isoformat()
        elif isinstance(expiration, datetime.timedelta):
            expires_in = ls_schemas.TimeDeltaInput(
                days=expiration.days,
                hours=expiration.seconds // 3600,
                minutes=(expiration.seconds // 60) % 60,
            )
        else:
            raise ValueError(f"Unknown expiration type: {type(expiration)}")
        # assemble body, one entry per key
        body: List[Dict[str, Any]] = [
            {
                "run_id": run_id,
                "feedback_key": feedback_key,
                "feedback_config": feedback_config,
                "expires_in": expires_in,
                "expires_at": expires_at,
            }
            for feedback_key, feedback_config in zip(feedback_keys, feedback_configs)
        ]
        response = self.request_with_retries(
            "POST",
            "/feedback/tokens",
            data=_dumps_json(body),
        )
        ls_utils.raise_for_status_with_text(response)
        return [ls_schemas.FeedbackIngestToken(**part) for part in response.json()]

    def list_presigned_feedback_tokens(
        self,
        run_id: ID_TYPE,
        *,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.FeedbackIngestToken]:
        """List the feedback ingest tokens for a run.

        Args:
            run_id: The ID of the run to filter by.
            limit: The maximum number of tokens to return.

        Yields:
            FeedbackIngestToken
                The feedback ingest tokens.
        """
        params = {
            "run_id": _as_uuid(run_id, "run_id"),
            "limit": min(limit, 100) if limit is not None else 100,
        }
        for i, token in enumerate(
            self._get_paginated_list("/feedback/tokens", params=params)
        ):
            yield ls_schemas.FeedbackIngestToken(**token)
            if limit is not None and i + 1 >= limit:
                break

    # Annotation Queue API

    def list_annotation_queues(
        self,
        *,
        queue_ids: Optional[List[ID_TYPE]] = None,
        name: Optional[str] = None,
        name_contains: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ls_schemas.AnnotationQueue]:
        """List the annotation queues on the LangSmith API.

        Args:
            queue_ids : List[str or UUID] or None, default=None
                The IDs of the queues to filter by.
            name : str or None, default=None
                The name of the queue to filter by.
            name_contains : str or None, default=None
                The substring that the queue name should contain.
            limit : int or None, default=None

        Yields:
            AnnotationQueue
                The annotation queues.
        """
        params: dict = {
            "ids": (
                [_as_uuid(id_, f"queue_ids[{i}]") for i, id_ in enumerate(queue_ids)]
                if queue_ids is not None
                else None
            ),
            "name": name,
            "name_contains": name_contains,
            "limit": min(limit, 100) if limit is not None else 100,
        }
        for i, queue in enumerate(
            self._get_paginated_list("/annotation-queues", params=params)
        ):
            yield ls_schemas.AnnotationQueue(
                **queue,
            )
            if limit is not None and i + 1 >= limit:
                break

    def create_annotation_queue(
        self,
        *,
        name: str,
        description: Optional[str] = None,
        queue_id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.AnnotationQueue:
        """Create an annotation queue on the LangSmith API.

        Args:
            name : str
                The name of the annotation queue.
            description : str, optional
                The description of the annotation queue.
            queue_id : str or UUID, optional
                The ID of the annotation queue.

        Returns:
            AnnotationQueue
                The created annotation queue object.
        """
        body = {
            "name": name,
            "description": description,
            "id": queue_id or str(uuid.uuid4()),
        }
        response = self.request_with_retries(
            "POST",
            "/annotation-queues",
            json={k: v for k, v in body.items() if v is not None},
        )
        ls_utils.raise_for_status_with_text(response)
        return ls_schemas.AnnotationQueue(
            **response.json(),
        )

    def read_annotation_queue(self, queue_id: ID_TYPE) -> ls_schemas.AnnotationQueue:
        """Read an annotation queue with the specified queue ID.

        Args:
            queue_id (ID_TYPE): The ID of the annotation queue to read.

        Returns:
            ls_schemas.AnnotationQueue: The annotation queue object.
        """
        # TODO: Replace when actual endpoint is added
        return next(self.list_annotation_queues(queue_ids=[queue_id]))

    def update_annotation_queue(
        self, queue_id: ID_TYPE, *, name: str, description: Optional[str] = None
    ) -> None:
        """Update an annotation queue with the specified queue_id.

        Args:
            queue_id (ID_TYPE): The ID of the annotation queue to update.
            name (str): The new name for the annotation queue.
            description (Optional[str], optional): The new description for the
                annotation queue. Defaults to None.
        """
        response = self.request_with_retries(
            "PATCH",
            f"/annotation-queues/{_as_uuid(queue_id, 'queue_id')}",
            json={
                "name": name,
                "description": description,
            },
        )
        ls_utils.raise_for_status_with_text(response)

    def delete_annotation_queue(self, queue_id: ID_TYPE) -> None:
        """Delete an annotation queue with the specified queue ID.

        Args:
            queue_id (ID_TYPE): The ID of the annotation queue to delete.
        """
        response = self.request_with_retries(
            "DELETE",
            f"/annotation-queues/{_as_uuid(queue_id, 'queue_id')}",
            headers={"Accept": "application/json", **self._headers},
        )
        ls_utils.raise_for_status_with_text(response)

    def add_runs_to_annotation_queue(
        self, queue_id: ID_TYPE, *, run_ids: List[ID_TYPE]
    ) -> None:
        """Add runs to an annotation queue with the specified queue ID.

        Args:
            queue_id (ID_TYPE): The ID of the annotation queue.
            run_ids (List[ID_TYPE]): The IDs of the runs to be added to the annotation
                queue.
        """
        response = self.request_with_retries(
            "POST",
            f"/annotation-queues/{_as_uuid(queue_id, 'queue_id')}/runs",
            json=[str(_as_uuid(id_, f"run_ids[{i}]")) for i, id_ in enumerate(run_ids)],
        )
        ls_utils.raise_for_status_with_text(response)

    def list_runs_from_annotation_queue(
        self, queue_id: ID_TYPE, *, limit: Optional[int] = None
    ) -> Iterator[ls_schemas.RunWithAnnotationQueueInfo]:
        """List runs from an annotation queue with the specified queue ID.

        Args:
            queue_id (ID_TYPE): The ID of the annotation queue.

        Yields:
            ls_schemas.RunWithAnnotationQueueInfo: An iterator of runs from the
                annotation queue.
        """
        path = f"/annotation-queues/{_as_uuid(queue_id, 'queue_id')}/runs"
        limit_ = min(limit, 100) if limit is not None else 100
        for i, run in enumerate(
            self._get_paginated_list(
                path, params={"headers": self._headers, "limit": limit_}
            )
        ):
            yield ls_schemas.RunWithAnnotationQueueInfo(**run)
            if limit is not None and i + 1 >= limit:
                break

    def create_comparative_experiment(
        self,
        name: str,
        experiments: Sequence[ID_TYPE],
        *,
        reference_dataset: Optional[ID_TYPE] = None,
        description: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
        metadata: Optional[Dict[str, Any]] = None,
        id: Optional[ID_TYPE] = None,
    ) -> ls_schemas.ComparativeExperiment:
        """Create a comparative experiment on the LangSmith API.

        These experiments compare 2 or more experiment results over a shared dataset.

        Args:
            name: The name of the comparative experiment.
            experiments: The IDs of the experiments to compare.
            reference_dataset: The ID of the dataset these experiments are compared on.
            description: The description of the comparative experiment.
            created_at: The creation time of the comparative experiment.
            metadata: Additional metadata for the comparative experiment.

        Returns:
            The created comparative experiment object.
        """
        if not experiments:
            raise ValueError("At least one experiment is required.")
        if reference_dataset is None:
            # Get one of the experiments' reference dataset
            reference_dataset = self.read_project(
                project_id=experiments[0]
            ).reference_dataset_id
        if not reference_dataset:
            raise ValueError("A reference dataset is required.")
        body: Dict[str, Any] = {
            "id": id or str(uuid.uuid4()),
            "name": name,
            "experiment_ids": experiments,
            "reference_dataset_id": reference_dataset,
            "description": description,
            "created_at": created_at or datetime.datetime.now(datetime.timezone.utc),
            "extra": {},
        }
        if metadata is not None:
            body["extra"]["metadata"] = metadata
        ser = _dumps_json({k: v for k, v in body.items()})  # if v is not None})
        response = self.request_with_retries(
            "POST",
            "/datasets/comparative",
            request_kwargs={
                "data": ser,
            },
        )
        ls_utils.raise_for_status_with_text(response)
        response_d = response.json()
        return ls_schemas.ComparativeExperiment(**response_d)

    async def arun_on_dataset(
        self,
        dataset_name: str,
        llm_or_chain_factory: Any,
        *,
        evaluation: Optional[Any] = None,
        concurrency_level: int = 5,
        project_name: Optional[str] = None,
        project_metadata: Optional[Dict[str, Any]] = None,
        dataset_version: Optional[Union[datetime.datetime, str]] = None,
        verbose: bool = False,
        input_mapper: Optional[Callable[[Dict], Any]] = None,
        revision_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Asynchronously run the Chain or language model on a dataset.

        Store traces to the specified project name.

        Args:
            dataset_name: Name of the dataset to run the chain on.
            llm_or_chain_factory: Language model or Chain constructor to run
                over the dataset. The Chain constructor is used to permit
                independent calls on each example without carrying over state.
            evaluation: Optional evaluation configuration to use when evaluating
            concurrency_level: The number of async tasks to run concurrently.
            project_name: Name of the project to store the traces in.
                Defaults to a randomly generated name.
            project_metadata: Optional metadata to store with the project.
            dataset_version: Optional version identifier to run the dataset on.
                Can be a timestamp or a string tag.
            verbose: Whether to print progress.
            tags: Tags to add to each run in the project.
            input_mapper: A function to map to the inputs dictionary from an Example
                to the format expected by the model to be evaluated. This is useful if
                your model needs to deserialize more complex schema or if your dataset
                has inputs with keys that differ from what is expected by your chain
                or agent.
            revision_id: Optional revision identifier to assign this test run to
                track the performance of different versions of your system.

        Returns:
            A dictionary containing the run's project name and the
            resulting model outputs.

        For the synchronous version, see client.run_on_dataset.

        Examples:
        --------
        .. code-block:: python

            from langsmith import Client
            from langchain.chat_models import ChatOpenAI
            from langchain.chains import LLMChain
            from langchain.smith import RunEvalConfig


            # Chains may have memory. Passing in a constructor function lets the
            # evaluation framework avoid cross-contamination between runs.
            def construct_chain():
                llm = ChatOpenAI(temperature=0)
                chain = LLMChain.from_string(llm, "What's the answer to {your_input_key}")
                return chain


            # Load off-the-shelf evaluators via config or the EvaluatorType (string or enum)
            evaluation_config = RunEvalConfig(
                evaluators=[
                    "qa",  # "Correctness" against a reference answer
                    "embedding_distance",
                    RunEvalConfig.Criteria("helpfulness"),
                    RunEvalConfig.Criteria(
                        {
                            "fifth-grader-score": "Do you have to be smarter than a fifth grader to answer this question?"
                        }
                    ),
                ]
            )

            client = Client()
            await client.arun_on_dataset(
                "<my_dataset_name>",
                construct_chain,
                evaluation=evaluation_config,
            )

        You can also create custom evaluators by subclassing the
        :class:`StringEvaluator <langchain.evaluation.schema.StringEvaluator>`
        or LangSmith's `RunEvaluator` classes.

        .. code-block:: python

            from typing import Optional
            from langchain.evaluation import StringEvaluator


            class MyStringEvaluator(StringEvaluator):
                @property
                def requires_input(self) -> bool:
                    return False

                @property
                def requires_reference(self) -> bool:
                    return True

                @property
                def evaluation_name(self) -> str:
                    return "exact_match"

                def _evaluate_strings(
                    self, prediction, reference=None, input=None, **kwargs
                ) -> dict:
                    return {"score": prediction == reference}


            evaluation_config = RunEvalConfig(
                custom_evaluators=[MyStringEvaluator()],
            )

            await client.arun_on_dataset(
                "<my_dataset_name>",
                construct_chain,
                evaluation=evaluation_config,
            )
        """  # noqa: E501
        # warn as deprecated and to use `aevaluate` instead
        warnings.warn(
            "The `arun_on_dataset` method is deprecated and"
            " will be removed in a future version."
            "Please use the `aevaluate` method instead.",
            DeprecationWarning,
        )
        try:
            from langchain.smith import arun_on_dataset as _arun_on_dataset
        except ImportError:
            raise ImportError(
                "The client.arun_on_dataset function requires the langchain"
                "package to run.\nInstall with pip install langchain"
            )
        return await _arun_on_dataset(
            dataset_name=dataset_name,
            llm_or_chain_factory=llm_or_chain_factory,
            client=self,
            evaluation=evaluation,
            concurrency_level=concurrency_level,
            project_name=project_name,
            project_metadata=project_metadata,
            verbose=verbose,
            input_mapper=input_mapper,
            revision_id=revision_id,
            dataset_version=dataset_version,
            **kwargs,
        )

    def run_on_dataset(
        self,
        dataset_name: str,
        llm_or_chain_factory: Any,
        *,
        evaluation: Optional[Any] = None,
        concurrency_level: int = 5,
        project_name: Optional[str] = None,
        project_metadata: Optional[Dict[str, Any]] = None,
        dataset_version: Optional[Union[datetime.datetime, str]] = None,
        verbose: bool = False,
        input_mapper: Optional[Callable[[Dict], Any]] = None,
        revision_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Run the Chain or language model on a dataset.

        Store traces to the specified project name.

        Args:
            dataset_name: Name of the dataset to run the chain on.
            llm_or_chain_factory: Language model or Chain constructor to run
                over the dataset. The Chain constructor is used to permit
                independent calls on each example without carrying over state.
            evaluation: Configuration for evaluators to run on the
                results of the chain
            concurrency_level: The number of tasks to execute concurrently.
            project_name: Name of the project to store the traces in.
                Defaults to a randomly generated name.
            project_metadata: Metadata to store with the project.
            dataset_version: Optional version identifier to run the dataset on.
                Can be a timestamp or a string tag.
            verbose: Whether to print progress.
            tags: Tags to add to each run in the project.
            input_mapper: A function to map to the inputs dictionary from an Example
                to the format expected by the model to be evaluated. This is useful if
                your model needs to deserialize more complex schema or if your dataset
                has inputs with keys that differ from what is expected by your chain
                or agent.
            revision_id: Optional revision identifier to assign this test run to
                track the performance of different versions of your system.

        Returns:
            A dictionary containing the run's project name and the resulting model outputs.


        For the (usually faster) async version of this function, see `client.arun_on_dataset`.

        Examples:
        --------
        .. code-block:: python

            from langsmith import Client
            from langchain.chat_models import ChatOpenAI
            from langchain.chains import LLMChain
            from langchain.smith import RunEvalConfig


            # Chains may have memory. Passing in a constructor function lets the
            # evaluation framework avoid cross-contamination between runs.
            def construct_chain():
                llm = ChatOpenAI(temperature=0)
                chain = LLMChain.from_string(llm, "What's the answer to {your_input_key}")
                return chain


            # Load off-the-shelf evaluators via config or the EvaluatorType (string or enum)
            evaluation_config = RunEvalConfig(
                evaluators=[
                    "qa",  # "Correctness" against a reference answer
                    "embedding_distance",
                    RunEvalConfig.Criteria("helpfulness"),
                    RunEvalConfig.Criteria(
                        {
                            "fifth-grader-score": "Do you have to be smarter than a fifth grader to answer this question?"
                        }
                    ),
                ]
            )

            client = Client()
            client.run_on_dataset(
                "<my_dataset_name>",
                construct_chain,
                evaluation=evaluation_config,
            )

        You can also create custom evaluators by subclassing the
        :class:`StringEvaluator <langchain.evaluation.schema.StringEvaluator>`
        or LangSmith's `RunEvaluator` classes.

        .. code-block:: python

            from typing import Optional
            from langchain.evaluation import StringEvaluator


            class MyStringEvaluator(StringEvaluator):
                @property
                def requires_input(self) -> bool:
                    return False

                @property
                def requires_reference(self) -> bool:
                    return True

                @property
                def evaluation_name(self) -> str:
                    return "exact_match"

                def _evaluate_strings(
                    self, prediction, reference=None, input=None, **kwargs
                ) -> dict:
                    return {"score": prediction == reference}


            evaluation_config = RunEvalConfig(
                custom_evaluators=[MyStringEvaluator()],
            )

            client.run_on_dataset(
                "<my_dataset_name>",
                construct_chain,
                evaluation=evaluation_config,
            )
        """  # noqa: E501
        warnings.warn(
            "The `run_on_dataset` method is deprecated and"
            " will be removed in a future version."
            "Please use the `evaluate` method instead.",
            DeprecationWarning,
        )
        try:
            from langchain.smith import run_on_dataset as _run_on_dataset
        except ImportError:
            raise ImportError(
                "The client.run_on_dataset function requires the langchain"
                "package to run.\nInstall with pip install langchain"
            )
        return _run_on_dataset(
            dataset_name=dataset_name,
            llm_or_chain_factory=llm_or_chain_factory,
            concurrency_level=concurrency_level,
            client=self,
            evaluation=evaluation,
            project_name=project_name,
            project_metadata=project_metadata,
            verbose=verbose,
            input_mapper=input_mapper,
            revision_id=revision_id,
            dataset_version=dataset_version,
            **kwargs,
        )


def _tracing_thread_drain_queue(
    tracing_queue: Queue, limit: int = 100, block: bool = True
) -> List[TracingQueueItem]:
    next_batch: List[TracingQueueItem] = []
    try:
        # wait 250ms for the first item, then
        # - drain the queue with a 50ms block timeout
        # - stop draining if we hit the limit
        # shorter drain timeout is used instead of non-blocking calls to
        # avoid creating too many small batches
        if item := tracing_queue.get(block=block, timeout=0.25):
            next_batch.append(item)
        while item := tracing_queue.get(block=block, timeout=0.05):
            next_batch.append(item)
            if limit and len(next_batch) >= limit:
                break
    except Empty:
        pass
    return next_batch


def _tracing_thread_handle_batch(
    client: Client,
    tracing_queue: Queue,
    batch: List[TracingQueueItem],
) -> None:
    create = [it.item for it in batch if it.action == "create"]
    update = [it.item for it in batch if it.action == "update"]
    try:
        client.batch_ingest_runs(create=create, update=update, pre_sampled=True)
    except Exception:
        logger.error("Error in tracing queue", exc_info=True)
        # exceptions are logged elsewhere, but we need to make sure the
        # background thread continues to run
        pass
    finally:
        for _ in batch:
            tracing_queue.task_done()


_AUTO_SCALE_UP_QSIZE_TRIGGER = 1000
_AUTO_SCALE_UP_NTHREADS_LIMIT = 16
_AUTO_SCALE_DOWN_NEMPTY_TRIGGER = 4


def _ensure_ingest_config(
    info: ls_schemas.LangSmithInfo,
) -> ls_schemas.BatchIngestConfig:
    default_config = ls_schemas.BatchIngestConfig(
        size_limit_bytes=None,  # Note this field is not used here
        size_limit=100,
        scale_up_nthreads_limit=_AUTO_SCALE_UP_NTHREADS_LIMIT,
        scale_up_qsize_trigger=_AUTO_SCALE_UP_QSIZE_TRIGGER,
        scale_down_nempty_trigger=_AUTO_SCALE_DOWN_NEMPTY_TRIGGER,
    )
    if not info:
        return default_config
    try:
        if not info.batch_ingest_config:
            return default_config
        return info.batch_ingest_config
    except BaseException:
        return default_config


def _tracing_control_thread_func(client_ref: weakref.ref[Client]) -> None:
    client = client_ref()
    if client is None:
        return
    tracing_queue = client.tracing_queue
    assert tracing_queue is not None
    batch_ingest_config = _ensure_ingest_config(client.info)
    size_limit: int = batch_ingest_config["size_limit"]
    scale_up_nthreads_limit: int = batch_ingest_config["scale_up_nthreads_limit"]
    scale_up_qsize_trigger: int = batch_ingest_config["scale_up_qsize_trigger"]

    sub_threads: List[threading.Thread] = []
    # 1 for this func, 1 for getrefcount, 1 for _get_data_type_cached
    num_known_refs = 3

    # loop until
    while (
        # the main thread dies
        threading.main_thread().is_alive()
        # or we're the only remaining reference to the client
        and sys.getrefcount(client) > num_known_refs + len(sub_threads)
    ):
        for thread in sub_threads:
            if not thread.is_alive():
                sub_threads.remove(thread)
        if (
            len(sub_threads) < scale_up_nthreads_limit
            and tracing_queue.qsize() > scale_up_qsize_trigger
        ):
            new_thread = threading.Thread(
                target=_tracing_sub_thread_func,
                args=(weakref.ref(client),),
            )
            sub_threads.append(new_thread)
            new_thread.start()
        if next_batch := _tracing_thread_drain_queue(tracing_queue, limit=size_limit):
            _tracing_thread_handle_batch(client, tracing_queue, next_batch)
    # drain the queue on exit
    while next_batch := _tracing_thread_drain_queue(
        tracing_queue, limit=size_limit, block=False
    ):
        _tracing_thread_handle_batch(client, tracing_queue, next_batch)


def _tracing_sub_thread_func(
    client_ref: weakref.ref[Client],
) -> None:
    client = client_ref()
    if client is None:
        return
    try:
        if not client.info:
            return
    except BaseException as e:
        logger.debug("Error in tracing control thread: %s", e)
        return
    tracing_queue = client.tracing_queue
    assert tracing_queue is not None
    batch_ingest_config = _ensure_ingest_config(client.info)
    size_limit = batch_ingest_config.get("size_limit", 100)
    seen_successive_empty_queues = 0

    # loop until
    while (
        # the main thread dies
        threading.main_thread().is_alive()
        # or we've seen the queue empty 4 times in a row
        and seen_successive_empty_queues
        <= batch_ingest_config["scale_down_nempty_trigger"]
    ):
        if next_batch := _tracing_thread_drain_queue(tracing_queue, limit=size_limit):
            seen_successive_empty_queues = 0
            _tracing_thread_handle_batch(client, tracing_queue, next_batch)
        else:
            seen_successive_empty_queues += 1

    # drain the queue on exit
    while next_batch := _tracing_thread_drain_queue(
        tracing_queue, limit=size_limit, block=False
    ):
        _tracing_thread_handle_batch(client, tracing_queue, next_batch)
