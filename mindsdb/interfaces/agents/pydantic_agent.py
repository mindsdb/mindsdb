import json
import asyncio
import threading
import queue
from typing import Dict, Iterable, List, AsyncIterable
import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.langfuse import LangfuseClientWrapper
from mindsdb.interfaces.agents.constants import (
    USER_COLUMN,
    ASSISTANT_COLUMN,
    CONTEXT_COLUMN,
    TRACE_ID_COLUMN
)
from mindsdb.interfaces.agents.models import (
    AgentCompletion, StreamChunk,
    StartChunk, EndChunk, ErrorChunk, ContentChunk,
    ToolCallChunk, ToolResultChunk, FinalChunk, UserPromptChunk
)

# Import Pydantic AI components
from pydantic_ai import Agent as PydanticAIAgent
from pydantic_ai.messages import (
    PartStartEvent, PartDeltaEvent, FinalResultEvent,
    TextPartDelta, ToolCallPartDelta,
    FunctionToolCallEvent, FunctionToolResultEvent
)
from pydantic_ai.models.openai import OpenAIModel

logger = log.getLogger(__name__)


class PydanticAgent:
    """Experimental agent implementation using Pydantic instead of Langchain.

    This class is intended to replace LangchainAgent but maintains the same interface
    to support a smooth transition away from the Langchain dependency.
    """

    def __init__(self, agent: db.Agents, model: dict = None):
        """Initialize the PydanticAgent.

        Args:
            agent: The database agent record
            model: Optional model information
        """
        self.agent = agent
        self.model = model
        self.args = self._initialize_args()

        # Setup Langfuse for tracking
        self.langfuse_client_wrapper = LangfuseClientWrapper()
        self.run_completion_span = None

    def _initialize_args(self) -> dict:
        """Initialize arguments based on the agent's parameters."""
        args = self.agent.params.copy() if self.agent.params else {}
        args["model_name"] = self.agent.model_name
        args["provider"] = self.agent.provider

        # If using mindsdb provider, get prompt from model
        if self.agent.provider == "mindsdb" and self.model:
            prompt_template = (
                self.model["problem_definition"].get("using", {}).get("prompt_template")
            )
            if prompt_template is not None:
                args["prompt_template"] = prompt_template

        # Ensure we have a prompt template - don't default to RAG
        if args.get("prompt_template") is None:
            raise ValueError(
                "Please provide a `prompt_template` in the agent parameters"
            )

        return args

    def get_metadata(self) -> Dict:
        """Get metadata for tracing."""
        return {
            'provider': self.args["provider"],
            'model_name': self.args["model_name"],
            'skills': [rel.skill.name for rel in self.agent.skills_relationships],
            'user_id': ctx.user_id,
            'session_id': ctx.session_id,
            'company_id': ctx.company_id,
            'user_class': ctx.user_class,
            'email_confirmed': ctx.email_confirmed
        }

    def get_tags(self) -> List:
        """Get tags for tracing."""
        return [self.args["provider"], "pydantic"]

    async def _run_agent(self, user_prompt: str) -> AgentCompletion:
        """Run the Pydantic agent to get a completion.

        Args:
            user_prompt: The user's prompt

        Returns:
            AgentCompletion with results
        """
        try:
            # Create agent with OpenAI config
            llm_config = self._get_llm_config()

            # Create the agent
            agent = PydanticAIAgent(
                model=llm_config,
                system_prompt=self.args.get("prompt_template", "You are a helpful assistant.")
            )

            # Run the agent
            result = await agent.run(user_prompt)

            # Return the result as an AgentCompletion
            return AgentCompletion(
                content=str(result.data),
                context=[],  # No context for now
                trace_id=self.langfuse_client_wrapper.get_trace_id()
            )

        except Exception as e:
            logger.error(f"Error running agent: {str(e)}", exc_info=True)
            return AgentCompletion(
                content=f"Error: {str(e)}",
                context=[],
                trace_id=self.langfuse_client_wrapper.get_trace_id()
            )

    def _extract_user_prompt(self, messages: List[Dict]) -> str:
        """Extract the user prompt from messages.

        Args:
            messages: List of message dictionaries

        Returns:
            User prompt string
        """
        if not messages:
            return ""

        user_prompt = messages[-1].get(USER_COLUMN, "")
        if not user_prompt and len(messages) > 0:
            # Fall back to 'content' if USER_COLUMN isn't found
            user_prompt = messages[-1].get('content', "")

        return user_prompt

    def get_completion(self, messages, stream: bool = False):
        """Get completion from the Pydantic-based agent.

        Args:
            messages: List of message dictionaries
            stream: Whether to stream the response

        Returns:
            DataFrame with completion results or a generator for streaming
        """
        # Set up tracing
        metadata = self.get_metadata()
        tags = self.get_tags()

        self.langfuse_client_wrapper.setup_trace(
            name='api-completion',
            input=messages,
            tags=tags,
            metadata=metadata,
            user_id=ctx.user_id,
            session_id=ctx.session_id,
        )

        self.run_completion_span = self.langfuse_client_wrapper.start_span(
            name='run-completion',
            input=messages
        )

        if stream:
            return self._get_completion_stream(messages)

        # Extract the user prompt from the messages
        user_prompt = self._extract_user_prompt(messages)
        logger.info(f"PydanticAgent processing prompt: {user_prompt[:50]}...")

        # Run the agent using asyncio.run
        try:
            result = asyncio.run(self._run_agent(user_prompt))

            # Create a DataFrame response
            df = pd.DataFrame({
                ASSISTANT_COLUMN: [result.content],
                CONTEXT_COLUMN: [json.dumps(result.context)],
                TRACE_ID_COLUMN: result.trace_id
            })

            # End tracing
            self.langfuse_client_wrapper.end_span(span=self.run_completion_span, output=df)

            return df

        except Exception as e:
            logger.error(f"Error getting completion: {str(e)}", exc_info=True)

            # Create error response
            df = pd.DataFrame({
                ASSISTANT_COLUMN: [f"Error: {str(e)}"],
                CONTEXT_COLUMN: [json.dumps([])],
                TRACE_ID_COLUMN: self.langfuse_client_wrapper.get_trace_id()
            })

            # End tracing
            self.langfuse_client_wrapper.end_span(span=self.run_completion_span, output=df)

            return df

    def _get_llm_config(self) -> OpenAIModel:
        """Get OpenAI configuration based on agent settings.

        Returns:
            OpenAIConfig: Configuration for the OpenAI model

        Raises:
            ValueError: If an unsupported provider is specified
        """
        provider = self.args.get("provider", "").lower()
        model_name = self.args.get("model_name", "gpt-4o")

        if provider == "openai":
            return OpenAIModel(model_name=model_name)
        else:
            raise ValueError(f"Provider '{provider}' is not supported in PydanticAgent yet. Only OpenAI is currently supported.")

    async def _stream_user_prompt_node(self, node, run_ctx, trace_id: str) -> AsyncIterable[StreamChunk]:
        """Process a user prompt node.

        Args:
            node: The user prompt node
            run_ctx: The run context
            trace_id: The trace ID for tracking

        Yields:
            Formatted user prompt events
        """
        yield UserPromptChunk(
            content=node.user_prompt,
            trace_id=trace_id
        )

    async def _stream_model_request_node(self, node, run_ctx, trace_id: str) -> AsyncIterable[StreamChunk]:
        """Process a model request node, streaming tokens from the model.

        Args:
            node: The model request node
            run_ctx: The run context
            trace_id: The trace ID for tracking

        Yields:
            Formatted model request events
        """
        async with node.stream(run_ctx) as request_stream:
            async for event in request_stream:
                if isinstance(event, PartStartEvent):
                    # Part start event
                    yield StreamChunk(
                        type="part_start",
                        index=event.index,
                        trace_id=trace_id
                    )
                elif isinstance(event, PartDeltaEvent):
                    if isinstance(event.delta, TextPartDelta):
                        # Text delta - this is the actual content to show the user
                        if event.delta.content_delta:
                            yield ContentChunk(
                                content=event.delta.content_delta,
                                trace_id=trace_id
                            )
                    elif isinstance(event.delta, ToolCallPartDelta):
                        # Tool call delta
                        yield StreamChunk(
                            type="tool_call_delta",
                            args_delta=event.delta.args_delta,
                            trace_id=trace_id
                        )
                elif isinstance(event, FinalResultEvent):
                    # Final result from this node
                    if hasattr(event, "result") and hasattr(event.result, "content"):
                        yield StreamChunk(
                            type="model_result",
                            content=event.result.content,
                            trace_id=trace_id
                        )

    async def _stream_call_tools_node(self, node, run_ctx, trace_id: str) -> AsyncIterable[StreamChunk]:
        """Process a call tools node, where the model potentially calls tools.

        Args:
            node: The call tools node
            run_ctx: The run context
            trace_id: The trace ID for tracking

        Yields:
            Formatted tool call and result events
        """
        async with node.stream(run_ctx) as handle_stream:
            async for event in handle_stream:
                if isinstance(event, FunctionToolCallEvent):
                    # Function tool call
                    yield ToolCallChunk(
                        tool=event.part.tool_name,
                        args=event.part.args,
                        tool_call_id=event.part.tool_call_id,
                        trace_id=trace_id
                    )
                elif isinstance(event, FunctionToolResultEvent):
                    # Function tool result
                    yield ToolResultChunk(
                        content=str(event.result.content),
                        tool_call_id=event.tool_call_id,
                        trace_id=trace_id
                    )

    async def _stream_end_node(self, node, run_result, trace_id: str) -> AsyncIterable[StreamChunk]:
        """Process an end node, which marks the completion of the agent run.

        Args:
            node: The end node
            run_result: The final result of the run
            trace_id: The trace ID for tracking

        Yields:
            Formatted final result event
        """
        yield FinalChunk(
            content=str(run_result.data),
            trace_id=trace_id
        )

    async def _process_agent_stream(self, user_prompt: str, trace_id: str, output_queue: queue.Queue) -> None:
        """Process the agent stream in an async function and put chunks into a queue.

        Args:
            user_prompt: The user's prompt
            trace_id: The trace ID for tracking
            output_queue: Queue to put chunks into
        """
        try:
            # Create agent
            llm_config = self._get_llm_config()
            agent = PydanticAIAgent(
                model=llm_config,
                system_prompt=self.args.get("prompt_template", "You are a helpful assistant.")
            )

            # Process agent stream
            async with agent.iter(user_prompt) as run:
                async for node in run:
                    if PydanticAIAgent.is_user_prompt_node(node):
                        async for chunk in self._stream_user_prompt_node(node, run.ctx, trace_id):
                            output_queue.put(chunk.model_dump())

                    elif PydanticAIAgent.is_model_request_node(node):
                        async for chunk in self._stream_model_request_node(node, run.ctx, trace_id):
                            output_queue.put(chunk.model_dump())

                    elif PydanticAIAgent.is_call_tools_node(node):
                        async for chunk in self._stream_call_tools_node(node, run.ctx, trace_id):
                            output_queue.put(chunk.model_dump())

                    elif PydanticAIAgent.is_end_node(node):
                        async for chunk in self._stream_end_node(node, run.result, trace_id):
                            output_queue.put(chunk.model_dump())

        except Exception as e:
            # Add error to queue
            logger.error(f"Error in agent stream: {str(e)}", exc_info=True)
            error_chunk = ErrorChunk(
                content=f"Error: {str(e)}",
                trace_id=trace_id
            )
            output_queue.put(error_chunk.model_dump())
        finally:
            # Always add sentinel at the end
            output_queue.put(None)

    def _get_completion_stream(self, messages: List[dict]) -> Iterable[Dict]:
        """Stream completion chunks using Pydantic AI's streaming functionality.

        Uses a thread-safe queue and asyncio.run() to handle the async agent stream.

        Args:
            messages: List of message dictionaries

        Returns:
            Generator yielding completion chunks
        """
        # Start the stream
        trace_id = self.langfuse_client_wrapper.get_trace_id()

        # Initial chunk
        start_chunk = StartChunk(trace_id=trace_id)
        yield start_chunk.model_dump()

        # Extract the user prompt from the messages
        user_prompt = self._extract_user_prompt(messages)
        logger.info(f"Streaming response with Pydantic AI for prompt: {user_prompt[:50]}...")

        # Create a thread-safe queue for communication
        output_queue = queue.Queue()

        # Start background thread for async processing
        worker_thread = threading.Thread(
            target=lambda: asyncio.run(self._process_agent_stream(user_prompt, trace_id, output_queue)),
            daemon=True
        )
        worker_thread.start()

        # Stream chunks as they arrive
        done = False
        while not done:
            # First check if the worker is still alive
            if not worker_thread.is_alive() and output_queue.empty():
                done = True
                continue

            try:
                # Try to get a chunk without waiting
                chunk = output_queue.get_nowait()

                # Check for sentinel
                if chunk is None:
                    output_queue.task_done()
                    done = True
                    continue

                # Yield chunk to caller
                yield chunk
                output_queue.task_done()

            except queue.Empty:
                # If queue is empty but worker is still running, try again in the next loop
                continue
            except Exception as e:
                logger.error(f"Error handling chunk: {str(e)}", exc_info=True)
                error_chunk = ErrorChunk(
                    content=f"Chunk handling error: {str(e)}",
                    trace_id=trace_id
                )
                yield error_chunk.model_dump()
                done = True

        # End the stream
        end_chunk = EndChunk(trace_id=trace_id)
        yield end_chunk.model_dump()

        # End tracing
        self.langfuse_client_wrapper.end_span_stream(span=self.run_completion_span)
