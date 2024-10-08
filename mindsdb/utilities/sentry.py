# Prepare sentry.io for error and exception tracking
import sentry_sdk
import os
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Provide your sentry.io DSN here
SENTRY_IO_DSN = os.environ.get("SENTRY_IO_DSN", "")
# Define the environment
SENTRY_IO_ENVIRONMENT = os.environ.get("SENTRY_IO_ENVIRONMENT", "local").lower()
# This is set to our SHA when deployed so we know what version this occurred in
SENTRY_IO_RELEASE = os.environ.get("SENTRY_IO_RELEASE", "local").lower()
# How often to capture traces, 1.0 means 100%.
SENTRY_IO_TRACE_SAMPLE_RATE = float(os.environ.get("SENTRY_IO_TRACE_SAMPLE_RATE", "1.0"))
# How often to capture profiling, 1.0 means 100%.
SENTRY_IO_PROFILING_SAMPLE_RATE = float(os.environ.get("SENTRY_IO_PROFILING_SAMPLE_RATE", "1.0"))
# By default we have sentry.io enabled on all envs, except for local which is disabled by default
#   If you want to enable sentry.io on local for some reason (eg: profiling) please set SENTRY_IO_FORCE_RUN to true
SENTRY_IO_DISABLED = True if (os.environ.get("SENTRY_IO_DISABLED", "false").lower() == "true" or SENTRY_IO_ENVIRONMENT == "local") else False
SENTRY_IO_FORCE_RUN = True if os.environ.get("SENTRY_IO_FORCE_RUN", "false").lower() == "true" else False


# If we're not disabled, or if we have forced sentry to run
if SENTRY_IO_DSN and (not SENTRY_IO_DISABLED or SENTRY_IO_FORCE_RUN):
    logger.info("Sentry.io enabled")
    logger.info(f"SENTRY_IO_DSN: {SENTRY_IO_DSN}")
    logger.info(f"SENTRY_IO_ENVIRONMENT: {SENTRY_IO_ENVIRONMENT}")
    logger.info(f"SENTRY_IO_RELEASE: {SENTRY_IO_RELEASE}")
    logger.info(f"SENTRY_IO_TRACE_SAMPLE_RATE: {SENTRY_IO_TRACE_SAMPLE_RATE * 100}%")
    logger.info(f"SENTRY_IO_PROFILING_SAMPLE_RATE: {SENTRY_IO_PROFILING_SAMPLE_RATE * 100}%")

    sentry_sdk.init(
        dsn=SENTRY_IO_DSN,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for tracing.
        traces_sample_rate=SENTRY_IO_TRACE_SAMPLE_RATE,
        # Set profiles_sample_rate to 1.0 to profile 100%
        # of sampled transactions.
        # We recommend adjusting this value in production.
        profiles_sample_rate=SENTRY_IO_PROFILING_SAMPLE_RATE,
        # What environment we're on, by default development
        environment=SENTRY_IO_ENVIRONMENT,
        # What release/image/etc we're using, injected in Helm/Kubernetes to be the image tag
        release=SENTRY_IO_RELEASE,
    )
