# Copyright 2022-present MongoDB, Inc.
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
from __future__ import annotations

import os
import threading
import weakref

_HAS_REGISTER_AT_FORK = hasattr(os, "register_at_fork")

# References to instances of _create_lock
_forkable_locks: weakref.WeakSet[threading.Lock] = weakref.WeakSet()


def _create_lock() -> threading.Lock:
    """Represents a lock that is tracked upon instantiation using a WeakSet and
    reset by pymongo upon forking.
    """
    lock = threading.Lock()
    if _HAS_REGISTER_AT_FORK:
        _forkable_locks.add(lock)
    return lock


def _release_locks() -> None:
    # Completed the fork, reset all the locks in the child.
    for lock in _forkable_locks:
        if lock.locked():
            lock.release()
