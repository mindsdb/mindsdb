# Copyright 2009-2015 MongoDB, Inc.
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

"""Exceptions raised by the :mod:`gridfs` package"""
from __future__ import annotations

from pymongo.errors import PyMongoError


class GridFSError(PyMongoError):
    """Base class for all GridFS exceptions."""


class CorruptGridFile(GridFSError):
    """Raised when a file in :class:`~gridfs.GridFS` is malformed."""


class NoFile(GridFSError):
    """Raised when trying to read from a non-existent file."""


class FileExists(GridFSError):
    """Raised when trying to create a file that already exists."""
