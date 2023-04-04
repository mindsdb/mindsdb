from ..integration_tests.flows.conftest import *  # noqa: F403

"""
Temporary workaround to import these fixtures, but ideally they would
be stored in a path that is accessible from both integration_tests,
handler_tests, and any others that may be added in the future.
"""
sys.path.append("../../integration_tests/flows/conftest.py")  # noqa: F405
