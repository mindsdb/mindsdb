# Copyright 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
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

"""Tools for working with read concerns."""
from __future__ import annotations

from typing import Any, Optional


class ReadConcern:
    """ReadConcern

    :param level: (string) The read concern level specifies the level of
          isolation for read operations.  For example, a read operation using a
          read concern level of ``majority`` will only return data that has been
          written to a majority of nodes. If the level is left unspecified, the
          server default will be used.

    .. versionadded:: 3.2

    """

    def __init__(self, level: Optional[str] = None) -> None:
        if level is None or isinstance(level, str):
            self.__level = level
        else:
            raise TypeError("level must be a string or None.")

    @property
    def level(self) -> Optional[str]:
        """The read concern level."""
        return self.__level

    @property
    def ok_for_legacy(self) -> bool:
        """Return ``True`` if this read concern is compatible with
        old wire protocol versions.
        """
        return self.level is None or self.level == "local"

    @property
    def document(self) -> dict[str, Any]:
        """The document representation of this read concern.

        .. note::
          :class:`ReadConcern` is immutable. Mutating the value of
          :attr:`document` does not mutate this :class:`ReadConcern`.
        """
        doc = {}
        if self.__level:
            doc["level"] = self.level
        return doc

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ReadConcern):
            return self.document == other.document
        return NotImplemented

    def __repr__(self) -> str:
        if self.level:
            return "ReadConcern(%s)" % self.level
        return "ReadConcern()"


DEFAULT_READ_CONCERN = ReadConcern()
