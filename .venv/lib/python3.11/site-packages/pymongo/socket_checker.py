# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Select / poll helper"""
from __future__ import annotations

import errno
import select
import sys
from typing import Any, Optional, cast

# PYTHON-2320: Jython does not fully support poll on SSL sockets,
# https://bugs.jython.org/issue2900
_HAVE_POLL = hasattr(select, "poll") and not sys.platform.startswith("java")
_SelectError = getattr(select, "error", OSError)


def _errno_from_exception(exc: BaseException) -> Optional[int]:
    if hasattr(exc, "errno"):
        return cast(int, exc.errno)
    if exc.args:
        return cast(int, exc.args[0])
    return None


class SocketChecker:
    def __init__(self) -> None:
        self._poller: Optional[select.poll]
        if _HAVE_POLL:
            self._poller = select.poll()
        else:
            self._poller = None

    def select(
        self, sock: Any, read: bool = False, write: bool = False, timeout: Optional[float] = 0
    ) -> bool:
        """Select for reads or writes with a timeout in seconds (or None).

        Returns True if the socket is readable/writable, False on timeout.
        """
        res: Any
        while True:
            try:
                if self._poller:
                    mask = select.POLLERR | select.POLLHUP
                    if read:
                        mask = mask | select.POLLIN | select.POLLPRI
                    if write:
                        mask = mask | select.POLLOUT
                    self._poller.register(sock, mask)
                    try:
                        # poll() timeout is in milliseconds. select()
                        # timeout is in seconds.
                        timeout_ = None if timeout is None else timeout * 1000
                        res = self._poller.poll(timeout_)
                        # poll returns a possibly-empty list containing
                        # (fd, event) 2-tuples for the descriptors that have
                        # events or errors to report. Return True if the list
                        # is not empty.
                        return bool(res)
                    finally:
                        self._poller.unregister(sock)
                else:
                    rlist = [sock] if read else []
                    wlist = [sock] if write else []
                    res = select.select(rlist, wlist, [sock], timeout)
                    # select returns a 3-tuple of lists of objects that are
                    # ready: subsets of the first three arguments. Return
                    # True if any of the lists are not empty.
                    return any(res)
            except (_SelectError, OSError) as exc:  # type: ignore
                if _errno_from_exception(exc) in (errno.EINTR, errno.EAGAIN):
                    continue
                raise

    def socket_closed(self, sock: Any) -> bool:
        """Return True if we know socket has been closed, False otherwise."""
        try:
            return self.select(sock, read=True)
        except (RuntimeError, KeyError):
            # RuntimeError is raised during a concurrent poll. KeyError
            # is raised by unregister if the socket is not in the poller.
            # These errors should not be possible since we protect the
            # poller with a mutex.
            raise
        except ValueError:
            # ValueError is raised by register/unregister/select if the
            # socket file descriptor is negative or outside the range for
            # select (> 1023).
            return True
        except Exception:
            # Any other exceptions should be attributed to a closed
            # or invalid socket.
            return True
