# Copyright 2023-present MongoDB, Inc.
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

"""Azure helpers."""
from __future__ import annotations

import json
from typing import Any, Optional


def _get_azure_response(
    resource: str, client_id: Optional[str] = None, timeout: float = 5
) -> dict[str, Any]:
    # Deferred import to save overall import time.
    from urllib.request import Request, urlopen

    url = "http://169.254.169.254/metadata/identity/oauth2/token"
    url += "?api-version=2018-02-01"
    url += f"&resource={resource}"
    if client_id:
        url += f"&client_id={client_id}"
    headers = {"Metadata": "true", "Accept": "application/json"}
    request = Request(url, headers=headers)  # noqa: S310
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = response.status
            body = response.read().decode("utf8")
    except Exception as e:
        msg = "Failed to acquire IMDS access token: %s" % e
        raise ValueError(msg) from None

    if status != 200:
        msg = "Failed to acquire IMDS access token."
        raise ValueError(msg)
    try:
        data = json.loads(body)
    except Exception:
        raise ValueError("Azure IMDS response must be in JSON format.") from None

    for key in ["access_token", "expires_in"]:
        if not data.get(key):
            msg = "Azure IMDS response must contain %s, but was %s."
            msg = msg % (key, body)
            raise ValueError(msg)

    return data
