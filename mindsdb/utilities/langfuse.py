import os
import typing
from typing import TYPE_CHECKING

from mindsdb.utilities import log

if TYPE_CHECKING:
    from langfuse.callback import CallbackHandler
    from langfuse.client import StatefulSpanClient

logger = log.getLogger(__name__)

# Define Langfuse public key.
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "langfuse_public_key")

# Define Langfuse secret key.
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "langfuse_secret_key")

# Define Langfuse host.
LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "http://localhost:3000")

# Define Langfuse environment.
LANGFUSE_ENVIRONMENT = os.getenv("LANGFUSE_ENVIRONMENT", "local")

# Define Langfuse release.
LANGFUSE_RELEASE = os.getenv("LANGFUSE_RELEASE", "local")

# Define Langfuse debug mode.
LANGFUSE_DEBUG = os.getenv("LANGFUSE_DEBUG", "false").lower() == "true"

# Define Langfuse timeout.
LANGFUSE_TIMEOUT = int(os.getenv("LANGFUSE_TIMEOUT", 10))

# Define Langfuse sample rate.
LANGFUSE_SAMPLE_RATE = float(os.getenv("LANGFUSE_SAMPLE_RATE", 1.0))

# Define if Langfuse is disabled.
LANGFUSE_DISABLED = os.getenv("LANGFUSE_DISABLED", "false").lower() == "true" or LANGFUSE_ENVIRONMENT == "local"
LANGFUSE_FORCE_RUN = os.getenv("LANGFUSE_FORCE_RUN", "false").lower() == "true"


class LangfuseClientWrapper:
    """
    Langfuse client wrapper. Defines Langfuse client configuration and initializes Langfuse client.
    """

    def __init__(self,
                 public_key: str = LANGFUSE_PUBLIC_KEY,
                 secret_key: str = LANGFUSE_SECRET_KEY,
                 host: str = LANGFUSE_HOST,
                 environment: str = LANGFUSE_ENVIRONMENT,
                 release: str = LANGFUSE_RELEASE,
                 debug: bool = LANGFUSE_DEBUG,
                 timeout: int = LANGFUSE_TIMEOUT,
                 sample_rate: float = LANGFUSE_SAMPLE_RATE,
                 disable: bool = LANGFUSE_DISABLED,
                 force_run: bool = LANGFUSE_FORCE_RUN) -> None:
        """
        Initialize Langfuse client.

        Args:
            public_key (str): Langfuse public key.
            secret_key (str): Langfuse secret key.
            host (str): Langfuse host.
            release (str): Langfuse release.
            timeout (int): Langfuse timeout.
            sample_rate (float): Langfuse sample rate.
        """

        self.metadata = None
        self.public_key = public_key
        self.secret_key = secret_key
        self.host = host
        self.environment = environment
        self.release = release
        self.debug = debug
        self.timeout = timeout
        self.sample_rate = sample_rate
        self.disable = disable
        self.force_run = force_run

        self.client = None
        self.trace = None
        self.metadata = None
        self.tags = None

        # Check if Langfuse is disabled.
        if LANGFUSE_DISABLED and not LANGFUSE_FORCE_RUN:
            logger.info("Langfuse is disabled.")
            return

        logger.info("Langfuse enabled")
        logger.debug(f"LANGFUSE_PUBLIC_KEY: {LANGFUSE_PUBLIC_KEY}")
        logger.debug(f"LANGFUSE_SECRET_KEY: {'*' * len(LANGFUSE_SECRET_KEY)}")
        logger.debug(f"LANGFUSE_HOST: {LANGFUSE_HOST}")
        logger.debug(f"LANGFUSE_ENVIRONMENT: {LANGFUSE_ENVIRONMENT}")
        logger.debug(f"LANGFUSE_RELEASE: {LANGFUSE_RELEASE}")
        logger.debug(f"LANGFUSE_DEBUG: {LANGFUSE_DEBUG}")
        logger.debug(f"LANGFUSE_TIMEOUT: {LANGFUSE_TIMEOUT}")
        logger.debug(f"LANGFUSE_SAMPLE_RATE: {LANGFUSE_SAMPLE_RATE * 100}%")

        try:
            from langfuse import Langfuse
        except ImportError:
            logger.error("Langfuse is not installed. Please install it with `pip install langfuse`.")
            return

        self.client = Langfuse(
            public_key=public_key,
            secret_key=secret_key,
            host=host,
            release=release,
            debug=debug,
            timeout=timeout,
            sample_rate=sample_rate
        )

    def setup_trace(self,
                    name: str,
                    input: typing.Optional[typing.Any] = None,
                    tags: typing.Optional[typing.List] = None,
                    metadata: typing.Optional[typing.Dict] = None,
                    user_id: str = None,
                    session_id: str = None) -> None:
        """
        Setup trace. If Langfuse is disabled, nothing will be done.
        Args:
            name (str): Trace name.
            input (dict): Trace input.
            tags (dict): Trace tags.
            metadata (dict): Trace metadata.
            user_id (str): User ID.
            session_id (str): Session ID.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return

        self.set_metadata(metadata)
        self.set_tags(tags)

        try:
            self.trace = self.client.trace(
                name=name,
                input=input,
                metadata=self.metadata,
                tags=self.tags,
                user_id=user_id,
                session_id=session_id
            )
        except Exception as e:
            logger.error(f'Something went wrong while processing Langfuse trace {self.trace.id}: {str(e)}')

        logger.info(f"Langfuse trace configured with ID: {self.trace.id}")

    def get_trace_id(self) -> typing.Optional[str]:
        """
        Get trace ID. If Langfuse is disabled, returns None.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return ""

        if self.trace is None:
            logger.debug("Langfuse trace is not setup.")
            return ""

        return self.trace.id

    def start_span(self,
                   name: str,
                   input: typing.Optional[typing.Any] = None) -> typing.Optional['StatefulSpanClient']:
        """
        Create span. If Langfuse is disabled, nothing will be done.

        Args:
            name (str): Span name.
            input (dict): Span input.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return None

        return self.trace.span(name=name, input=input)

    def end_span_stream(self,
                        span: typing.Optional['StatefulSpanClient'] = None) -> None:
        """
        End span. If Langfuse is disabled, nothing will happen.
        Args:
            span (Any): Span object.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return

        span.end()
        self.trace.update()

    def end_span(self,
                 span: typing.Optional['StatefulSpanClient'] = None,
                 output: typing.Optional[typing.Any] = None) -> None:
        """
        End trace. If Langfuse is disabled, nothing will be done.

        Args:
            span (Any): Span object.
            output (Any): Span output.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return

        if span is None:
            logger.debug("Langfuse span is not created.")
            return

        span.end(output=output)
        self.trace.update(output=output)

        metadata = self.metadata or {}

        try:
            # Ensure all batched traces are sent before fetching.
            self.client.flush()
            metadata['tool_usage'] = self._get_tool_usage()
            self.trace.update(metadata=metadata)

        except Exception as e:
            logger.error(f'Something went wrong while processing Langfuse trace {self.trace.id}: {str(e)}')

    def get_langchain_handler(self) -> typing.Optional['CallbackHandler']:
        """
        Get Langchain handler. If Langfuse is disabled, returns None.
        """

        if self.client is None:
            logger.debug("Langfuse is disabled.")
            return None

        return self.trace.get_langchain_handler()

    def set_metadata(self, custom_metadata: dict = None) -> None:
        """
        Get default metadata.
        """
        self.metadata = custom_metadata or {}

        self.metadata["environment"] = self.environment
        self.metadata["release"] = self.release

    def set_tags(self, custom_tags: typing.Optional[typing.List] = None) -> None:
        """
        Get default tags.
        """
        self.tags = custom_tags or []

        self.tags.append(self.environment)
        self.tags.append(self.release)

    def _get_tool_usage(self) -> typing.Dict:
        """Retrieves tool usage information from a langfuse trace.
        Note: assumes trace marks an action with string `AgentAction`
        """
        from langfuse.api.resources.commons.errors.not_found_error import NotFoundError as TraceNotFoundError

        tool_usage = {}

        try:
            fetched_trace = self.client.get_trace(self.trace.id)
            steps = [s.name for s in fetched_trace.observations]
            for step in steps:
                if 'AgentAction' in step:
                    tool_name = step.split('-')[1]
                    if tool_name not in tool_usage:
                        tool_usage[tool_name] = 0
                    tool_usage[tool_name] += 1
        except TraceNotFoundError:
            logger.warning(f'Langfuse trace {self.trace.id} not found')
        except Exception as e:
            logger.error(f'Something went wrong while processing Langfuse trace {self.trace.id}: {str(e)}')

        return tool_usage
