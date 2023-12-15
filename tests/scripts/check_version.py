import re
import sys

from mindsdb.__about__ import __version__

# PEP440: https://www.python.org/dev/peps/pep-0440/
RELEASE_PATTERN = "^\d+(\.\d+)*$"  # noqa: W605
PRERELEASE_PATTERN = "^\d+(\.\d+)*(a|b|rc)\d+$"  # noqa: W605

version_str = sys.argv[1].replace('v', '')
is_prerelease = sys.argv[2] == "true"

if is_prerelease:
    if re.match(PRERELEASE_PATTERN, version_str) is None:
        raise Exception("Invalid prerelease version: %s" % version_str)
elif re.match(RELEASE_PATTERN, version_str) is None:
    raise Exception("Invalid release version: %s" % version_str)


if version_str != __version__:
    raise Exception("Version mismatch between __about__.py and release tag: %s != %s" % (version_str, __version__))

print(version_str)
