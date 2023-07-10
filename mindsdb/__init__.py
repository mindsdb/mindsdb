# Do this as early as possible
import logging

from mindsdb.__about__ import __version__  # noqa: F401
from mindsdb.utilities import log

log.configure_logging()
