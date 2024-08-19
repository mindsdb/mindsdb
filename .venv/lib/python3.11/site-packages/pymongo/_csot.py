# Copyright 2022-present MongoDB, Inc.
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

"""Internal helpers for CSOT."""

from __future__ import annotations

import functools
import time
from collections import deque
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, Callable, Deque, MutableMapping, Optional, TypeVar, cast

if TYPE_CHECKING:
    from pymongo.write_concern import WriteConcern

TIMEOUT: ContextVar[Optional[float]] = ContextVar("TIMEOUT", default=None)
RTT: ContextVar[float] = ContextVar("RTT", default=0.0)
DEADLINE: ContextVar[float] = ContextVar("DEADLINE", default=float("inf"))


def get_timeout() -> Optional[float]:
    return TIMEOUT.get(None)


def get_rtt() -> float:
    return RTT.get()


def get_deadline() -> float:
    return DEADLINE.get()


def set_rtt(rtt: float) -> None:
    RTT.set(rtt)


def remaining() -> Optional[float]:
    if not get_timeout():
        return None
    return DEADLINE.get() - time.monotonic()


def clamp_remaining(max_timeout: float) -> float:
    """Return the remaining timeout clamped to a max value."""
    timeout = remaining()
    if timeout is None:
        return max_timeout
    return min(timeout, max_timeout)


class _TimeoutContext(AbstractContextManager):
    """Internal timeout context manager.

    Use :func:`pymongo.timeout` instead::

      with pymongo.timeout(0.5):
          client.test.test.insert_one({})
    """

    def __init__(self, timeout: Optional[float]):
        self._timeout = timeout
        self._tokens: Optional[tuple[Token[Optional[float]], Token[float], Token[float]]] = None

    def __enter__(self) -> _TimeoutContext:
        timeout_token = TIMEOUT.set(self._timeout)
        prev_deadline = DEADLINE.get()
        next_deadline = time.monotonic() + self._timeout if self._timeout else float("inf")
        deadline_token = DEADLINE.set(min(prev_deadline, next_deadline))
        rtt_token = RTT.set(0.0)
        self._tokens = (timeout_token, deadline_token, rtt_token)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._tokens:
            timeout_token, deadline_token, rtt_token = self._tokens
            TIMEOUT.reset(timeout_token)
            DEADLINE.reset(deadline_token)
            RTT.reset(rtt_token)


# See https://mypy.readthedocs.io/en/stable/generics.html?#decorator-factories
F = TypeVar("F", bound=Callable[..., Any])


def apply(func: F) -> F:
    """Apply the client's timeoutMS to this operation."""

    @functools.wraps(func)
    def csot_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        if get_timeout() is None:
            timeout = self._timeout
            if timeout is not None:
                with _TimeoutContext(timeout):
                    return func(self, *args, **kwargs)
        return func(self, *args, **kwargs)

    return cast(F, csot_wrapper)


def apply_write_concern(
    cmd: MutableMapping[str, Any], write_concern: Optional[WriteConcern]
) -> None:
    """Apply the given write concern to a command."""
    if not write_concern or write_concern.is_server_default:
        return
    wc = write_concern.document
    if get_timeout() is not None:
        wc.pop("wtimeout", None)
    if wc:
        cmd["writeConcern"] = wc


_MAX_RTT_SAMPLES: int = 10
_MIN_RTT_SAMPLES: int = 2


class MovingMinimum:
    """Tracks a minimum RTT within the last 10 RTT samples."""

    samples: Deque[float]

    def __init__(self) -> None:
        self.samples = deque(maxlen=_MAX_RTT_SAMPLES)

    def add_sample(self, sample: float) -> None:
        if sample < 0:
            # Likely system time change while waiting for hello response
            # and not using time.monotonic. Ignore it, the next one will
            # probably be valid.
            return
        self.samples.append(sample)

    def get(self) -> float:
        """Get the min, or 0.0 if there aren't enough samples yet."""
        if len(self.samples) >= _MIN_RTT_SAMPLES:
            return min(self.samples)
        return 0.0

    def reset(self) -> None:
        self.samples.clear()
