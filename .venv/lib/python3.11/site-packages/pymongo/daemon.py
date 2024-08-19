# Copyright 2019-present MongoDB, Inc.
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

"""Support for spawning a daemon process.

PyMongo only attempts to spawn the mongocryptd daemon process when automatic
client-side field level encryption is enabled. See
:ref:`automatic-client-side-encryption` for more info.
"""
from __future__ import annotations

import os
import subprocess
import sys
import warnings
from typing import Any, Optional, Sequence

# The maximum amount of time to wait for the intermediate subprocess.
_WAIT_TIMEOUT = 10
_THIS_FILE = os.path.realpath(__file__)


def _popen_wait(popen: subprocess.Popen[Any], timeout: Optional[float]) -> Optional[int]:
    """Implement wait timeout support for Python 3."""
    try:
        return popen.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Silence TimeoutExpired errors.
        return None


def _silence_resource_warning(popen: Optional[subprocess.Popen[Any]]) -> None:
    """Silence Popen's ResourceWarning.

    Note this should only be used if the process was created as a daemon.
    """
    # Set the returncode to avoid this warning when popen is garbage collected:
    # "ResourceWarning: subprocess XXX is still running".
    # See https://bugs.python.org/issue38890 and
    # https://bugs.python.org/issue26741.
    # popen is None when mongocryptd spawning fails
    if popen is not None:
        popen.returncode = 0


if sys.platform == "win32":
    # On Windows we spawn the daemon process simply by using DETACHED_PROCESS.
    _DETACHED_PROCESS = getattr(subprocess, "DETACHED_PROCESS", 0x00000008)

    def _spawn_daemon(args: Sequence[str]) -> None:
        """Spawn a daemon process (Windows)."""
        try:
            with open(os.devnull, "r+b") as devnull:
                popen = subprocess.Popen(
                    args,  # noqa: S603
                    creationflags=_DETACHED_PROCESS,
                    stdin=devnull,
                    stderr=devnull,
                    stdout=devnull,
                )
                _silence_resource_warning(popen)
        except FileNotFoundError as exc:
            warnings.warn(
                f"Failed to start {args[0]}: is it on your $PATH?\nOriginal exception: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )

else:
    # On Unix we spawn the daemon process with a double Popen.
    # 1) The first Popen runs this file as a Python script using the current
    #    interpreter.
    # 2) The script then decouples itself and performs the second Popen to
    #    spawn the daemon process.
    # 3) The original process waits up to 10 seconds for the script to exit.
    #
    # Note that we do not call fork() directly because we want this procedure
    # to be safe to call from any thread. Using Popen instead of fork also
    # avoids triggering the application's os.register_at_fork() callbacks when
    # we spawn the mongocryptd daemon process.
    def _spawn(args: Sequence[str]) -> Optional[subprocess.Popen[Any]]:
        """Spawn the process and silence stdout/stderr."""
        try:
            with open(os.devnull, "r+b") as devnull:
                return subprocess.Popen(
                    args,  # noqa: S603
                    close_fds=True,
                    stdin=devnull,
                    stderr=devnull,
                    stdout=devnull,
                )
        except FileNotFoundError as exc:
            warnings.warn(
                f"Failed to start {args[0]}: is it on your $PATH?\nOriginal exception: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
        return None

    def _spawn_daemon_double_popen(args: Sequence[str]) -> None:
        """Spawn a daemon process using a double subprocess.Popen."""
        spawner_args = [sys.executable, _THIS_FILE]
        spawner_args.extend(args)
        temp_proc = subprocess.Popen(spawner_args, close_fds=True)  # noqa: S603
        # Reap the intermediate child process to avoid creating zombie
        # processes.
        _popen_wait(temp_proc, _WAIT_TIMEOUT)

    def _spawn_daemon(args: Sequence[str]) -> None:
        """Spawn a daemon process (Unix)."""
        # "If Python is unable to retrieve the real path to its executable,
        # sys.executable will be an empty string or None".
        if sys.executable:
            _spawn_daemon_double_popen(args)
        else:
            # Fallback to spawn a non-daemon process without silencing the
            # resource warning. We do not use fork here because it is not
            # safe to call from a thread on all systems.
            # Unfortunately, this means that:
            # 1) If the parent application is killed via Ctrl-C, the
            #    non-daemon process will also be killed.
            # 2) Each non-daemon process will hang around as a zombie process
            #    until the main application exits.
            _spawn(args)

    if __name__ == "__main__":
        # Attempt to start a new session to decouple from the parent.
        if hasattr(os, "setsid"):
            try:
                os.setsid()
            except OSError:
                pass

        # We are performing a double fork (Popen) to spawn the process as a
        # daemon so it is safe to ignore the resource warning.
        _silence_resource_warning(_spawn(sys.argv[1:]))
        os._exit(0)
