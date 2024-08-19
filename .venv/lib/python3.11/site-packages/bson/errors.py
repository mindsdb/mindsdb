# Copyright 2009-present MongoDB, Inc.
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

"""Exceptions raised by the BSON package."""
from __future__ import annotations


class BSONError(Exception):
    """Base class for all BSON exceptions."""


class InvalidBSON(BSONError):
    """Raised when trying to create a BSON object from invalid data."""


class InvalidStringData(BSONError):
    """Raised when trying to encode a string containing non-UTF8 data."""


class InvalidDocument(BSONError):
    """Raised when trying to create a BSON object from an invalid document."""


class InvalidId(BSONError):
    """Raised when trying to create an ObjectId from invalid data."""
