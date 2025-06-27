import os

# By default, we have Open Telemetry SDK enabled on all envs, except for local which is disabled by default.
OTEL_SDK_DISABLED = (os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true"
                     or os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower() == "local")

# If you want to enable Open Telemetry on local for some reason please set OTEL_SDK_FORCE_RUN to true
OTEL_SDK_FORCE_RUN = os.getenv("OTEL_SDK_FORCE_RUN", "false").lower() == "true"

OTEL_ENABLED = not OTEL_SDK_DISABLED or OTEL_SDK_FORCE_RUN

def increment_otel_query_request_counter(metadata: dict) -> None:
    pass

trace = None
if OTEL_ENABLED:
    try:
        from mindsdb.utilities.otel.prepare import trace
        from mindsdb.utilities.otel.metric_handlers import increment_otel_query_request_counter
    except Exception:
        pass

