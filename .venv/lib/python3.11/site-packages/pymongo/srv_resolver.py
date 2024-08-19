# Copyright 2019-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Support for resolving hosts and options from mongodb+srv:// URIs."""
from __future__ import annotations

import ipaddress
import random
from typing import TYPE_CHECKING, Any, Optional, Union

from pymongo.common import CONNECT_TIMEOUT
from pymongo.errors import ConfigurationError

if TYPE_CHECKING:
    from dns import resolver


def _have_dnspython() -> bool:
    try:
        import dns  # noqa: F401

        return True
    except ImportError:
        return False


# dnspython can return bytes or str from various parts
# of its API depending on version. We always want str.
def maybe_decode(text: Union[str, bytes]) -> str:
    if isinstance(text, bytes):
        return text.decode()
    return text


# PYTHON-2667 Lazily call dns.resolver methods for compatibility with eventlet.
def _resolve(*args: Any, **kwargs: Any) -> resolver.Answer:
    from dns import resolver

    if hasattr(resolver, "resolve"):
        # dnspython >= 2
        return resolver.resolve(*args, **kwargs)
    # dnspython 1.X
    return resolver.query(*args, **kwargs)


_INVALID_HOST_MSG = (
    "Invalid URI host: %s is not a valid hostname for 'mongodb+srv://'. "
    "Did you mean to use 'mongodb://'?"
)


class _SrvResolver:
    def __init__(
        self,
        fqdn: str,
        connect_timeout: Optional[float],
        srv_service_name: str,
        srv_max_hosts: int = 0,
    ):
        self.__fqdn = fqdn
        self.__srv = srv_service_name
        self.__connect_timeout = connect_timeout or CONNECT_TIMEOUT
        self.__srv_max_hosts = srv_max_hosts or 0
        # Validate the fully qualified domain name.
        try:
            ipaddress.ip_address(fqdn)
            raise ConfigurationError(_INVALID_HOST_MSG % ("an IP address",))
        except ValueError:
            pass

        try:
            self.__plist = self.__fqdn.split(".")[1:]
        except Exception:
            raise ConfigurationError(_INVALID_HOST_MSG % (fqdn,)) from None
        self.__slen = len(self.__plist)
        if self.__slen < 2:
            raise ConfigurationError(_INVALID_HOST_MSG % (fqdn,))

    def get_options(self) -> Optional[str]:
        from dns import resolver

        try:
            results = _resolve(self.__fqdn, "TXT", lifetime=self.__connect_timeout)
        except (resolver.NoAnswer, resolver.NXDOMAIN):
            # No TXT records
            return None
        except Exception as exc:
            raise ConfigurationError(str(exc)) from None
        if len(results) > 1:
            raise ConfigurationError("Only one TXT record is supported")
        return (b"&".join([b"".join(res.strings) for res in results])).decode("utf-8")

    def _resolve_uri(self, encapsulate_errors: bool) -> resolver.Answer:
        try:
            results = _resolve(
                "_" + self.__srv + "._tcp." + self.__fqdn, "SRV", lifetime=self.__connect_timeout
            )
        except Exception as exc:
            if not encapsulate_errors:
                # Raise the original error.
                raise
            # Else, raise all errors as ConfigurationError.
            raise ConfigurationError(str(exc)) from None
        return results

    def _get_srv_response_and_hosts(
        self, encapsulate_errors: bool
    ) -> tuple[resolver.Answer, list[tuple[str, Any]]]:
        results = self._resolve_uri(encapsulate_errors)

        # Construct address tuples
        nodes = [
            (maybe_decode(res.target.to_text(omit_final_dot=True)), res.port) for res in results
        ]

        # Validate hosts
        for node in nodes:
            try:
                nlist = node[0].lower().split(".")[1:][-self.__slen :]
            except Exception:
                raise ConfigurationError(f"Invalid SRV host: {node[0]}") from None
            if self.__plist != nlist:
                raise ConfigurationError(f"Invalid SRV host: {node[0]}")
        if self.__srv_max_hosts:
            nodes = random.sample(nodes, min(self.__srv_max_hosts, len(nodes)))
        return results, nodes

    def get_hosts(self) -> list[tuple[str, Any]]:
        _, nodes = self._get_srv_response_and_hosts(True)
        return nodes

    def get_hosts_and_min_ttl(self) -> tuple[list[tuple[str, Any]], int]:
        results, nodes = self._get_srv_response_and_hosts(False)
        rrset = results.rrset
        ttl = rrset.ttl if rrset else 0
        return nodes, ttl
