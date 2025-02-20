from typing import Any, Dict, Union, Optional, List
from uuid import uuid4
import datetime
import json

from langchain_core.callbacks.base import BaseCallbackHandler

from mindsdb.utilities import log
from mindsdb.interfaces.storage import db

logger = log.getLogger(__name__)
logger.setLevel('DEBUG')


class LangfuseCallbackHandler(BaseCallbackHandler):
    """Langchain callback handler that traces tool & chain executions using Langfuse."""

    def __init__(self, langfuse, trace_id: Optional[str] = None, observation_id: Optional[str] = None):
        self.langfuse = langfuse
        self.chain_uuid_to_span = {}
        self.action_uuid_to_span = {}
        # if these are not available, we generate some UUIDs
        self.trace_id = trace_id or uuid4().hex
        self.observation_id = observation_id or uuid4().hex
        # Track metrics about tools and chains
        self.tool_metrics = {}
        self.chain_metrics = {}
        self.current_chain = None

    def on_tool_start(
            self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> Any:
        """Run when tool starts running."""
        parent_run_uuid = kwargs.get('parent_run_id', uuid4()).hex
        action_span = self.action_uuid_to_span.get(parent_run_uuid)
        if action_span is None:
            return

        tool_name = serialized.get("name", "tool")
        start_time = datetime.datetime.now()

        # Initialize or update tool metrics
        if tool_name not in self.tool_metrics:
            self.tool_metrics[tool_name] = {
                'count': 0,
                'total_time': 0,
                'errors': 0,
                'last_error': None,
                'inputs': []
            }

        self.tool_metrics[tool_name]['count'] += 1
        self.tool_metrics[tool_name]['inputs'].append(input_str)

        metadata = {
            'tool_name': tool_name,
            'started': start_time.isoformat(),
            'start_timestamp': start_time.timestamp(),
            'input_length': len(input_str) if input_str else 0
        }
        action_span.update(metadata=metadata)

    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        """Run when tool ends running."""
        parent_run_uuid = kwargs.get('parent_run_id', uuid4()).hex
        action_span = self.action_uuid_to_span.get(parent_run_uuid)
        if action_span is None:
            return

        end_time = datetime.datetime.now()
        tool_name = action_span.metadata.get('tool_name', 'unknown')
        start_timestamp = action_span.metadata.get('start_timestamp')

        if start_timestamp:
            duration = end_time.timestamp() - start_timestamp
            if tool_name in self.tool_metrics:
                self.tool_metrics[tool_name]['total_time'] += duration

        metadata = {
            'finished': end_time.isoformat(),
            'duration_seconds': duration if start_timestamp else None,
            'output_length': len(output) if output else 0
        }

        action_span.update(
            output=output,  # tool output is action output (unless superseded by a global action output)
            metadata=metadata
        )

    def on_tool_error(
            self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        """Run when tool errors."""
        parent_run_uuid = kwargs.get('parent_run_id', uuid4()).hex
        action_span = self.action_uuid_to_span.get(parent_run_uuid)
        if action_span is None:
            return

        try:
            error_str = str(error)
        except Exception:
            error_str = "Couldn't get error string."

        tool_name = action_span.metadata.get('tool_name', 'unknown')
        if tool_name in self.tool_metrics:
            self.tool_metrics[tool_name]['errors'] += 1
            self.tool_metrics[tool_name]['last_error'] = error_str

        metadata = {
            'error_description': error_str,
            'error_type': error.__class__.__name__,
            'error_time': datetime.datetime.now().isoformat()
        }
        action_span.update(metadata=metadata)

    def on_chain_start(
            self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any
    ) -> Any:
        """Run when chain starts running."""
        if self.langfuse is None:
            return

        run_uuid = kwargs.get('run_id', uuid4()).hex

        if serialized is None:
            serialized = {}

        chain_name = serialized.get("name", "chain")
        start_time = datetime.datetime.now()

        # Initialize or update chain metrics
        if chain_name not in self.chain_metrics:
            self.chain_metrics[chain_name] = {
                'count': 0,
                'total_time': 0,
                'errors': 0,
                'last_error': None
            }

        self.chain_metrics[chain_name]['count'] += 1
        self.current_chain = chain_name

        try:
            chain_span = self.langfuse.span(
                name=f'{chain_name}-{run_uuid}',
                trace_id=self.trace_id,
                parent_observation_id=self.observation_id,
                input=json.dumps(inputs, indent=2)
            )

            metadata = {
                'chain_name': chain_name,
                'started': start_time.isoformat(),
                'start_timestamp': start_time.timestamp(),
                'input_keys': list(inputs.keys()) if isinstance(inputs, dict) else None,
                'input_size': len(inputs) if isinstance(inputs, dict) else len(str(inputs))
            }
            chain_span.update(metadata=metadata)
            self.chain_uuid_to_span[run_uuid] = chain_span
        except Exception as e:
            logger.warning(f"Error creating Langfuse span: {str(e)}")

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> Any:
        """Run when chain ends running."""
        if self.langfuse is None:
            return

        chain_uuid = kwargs.get('run_id', uuid4()).hex
        if chain_uuid not in self.chain_uuid_to_span:
            return
        chain_span = self.chain_uuid_to_span.pop(chain_uuid)
        if chain_span is None:
            return

        try:
            end_time = datetime.datetime.now()
            chain_name = chain_span.metadata.get('chain_name', 'unknown')
            start_timestamp = chain_span.metadata.get('start_timestamp')

            if start_timestamp and chain_name in self.chain_metrics:
                duration = end_time.timestamp() - start_timestamp
                self.chain_metrics[chain_name]['total_time'] += duration

            metadata = {
                'finished': end_time.isoformat(),
                'duration_seconds': duration if start_timestamp else None,
                'output_keys': list(outputs.keys()) if isinstance(outputs, dict) else None,
                'output_size': len(outputs) if isinstance(outputs, dict) else len(str(outputs))
            }
            chain_span.update(output=json.dumps(outputs, indent=2), metadata=metadata)
            chain_span.end()
        except Exception as e:
            logger.warning(f"Error updating Langfuse span: {str(e)}")

    def on_chain_error(self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any) -> Any:
        """Run when chain errors."""
        chain_uuid = kwargs.get('run_id', uuid4()).hex
        if chain_uuid not in self.chain_uuid_to_span:
            return
        chain_span = self.chain_uuid_to_span.get(chain_uuid)
        if chain_span is None:
            return

        try:
            error_str = str(error)
        except Exception:
            error_str = "Couldn't get error string."

        chain_name = chain_span.metadata.get('chain_name', 'unknown')
        if chain_name in self.chain_metrics:
            self.chain_metrics[chain_name]['errors'] += 1
            self.chain_metrics[chain_name]['last_error'] = error_str

        metadata = {
            'error_description': error_str,
            'error_type': error.__class__.__name__,
            'error_time': datetime.datetime.now().isoformat()
        }
        chain_span.update(metadata=metadata)

    def on_agent_action(self, action, **kwargs: Any) -> Any:
        """Run on agent action."""
        if self.langfuse is None:
            return

        run_uuid = kwargs.get('run_id', uuid4()).hex
        try:
            action_span = self.langfuse.span(
                name=f'{getattr(action, "type", "action")}-{getattr(action, "tool", "")}-{run_uuid}',
                trace_id=self.trace_id,
                parent_observation_id=self.observation_id,
                input=str(action)
            )
            self.action_uuid_to_span[run_uuid] = action_span
        except Exception as e:
            logger.warning(f"Error creating Langfuse span for agent action: {str(e)}")

    def on_agent_finish(self, finish, **kwargs: Any) -> Any:
        """Run on agent end."""
        if self.langfuse is None:
            return

        run_uuid = kwargs.get('run_id', uuid4()).hex
        if run_uuid not in self.action_uuid_to_span:
            return
        action_span = self.action_uuid_to_span.pop(run_uuid)
        if action_span is None:
            return

        try:
            if finish is not None:
                action_span.update(output=finish)  # supersedes tool output
            action_span.end()
        except Exception as e:
            logger.warning(f"Error updating Langfuse span: {str(e)}")

    def auth_check(self):
        if self.langfuse is not None:
            return self.langfuse.auth_check()
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics about tools and chains.

        Returns:
            Dict containing:
            - tool_metrics: Statistics about tool usage, errors, and timing
            - chain_metrics: Statistics about chain execution, errors, and timing
            For each tool/chain, includes:
                - count: Number of times used
                - total_time: Total execution time
                - errors: Number of errors
                - last_error: Most recent error message
                - avg_duration: Average execution time
        """
        metrics = {
            'tool_metrics': {},
            'chain_metrics': {}
        }

        # Process tool metrics
        for tool_name, data in self.tool_metrics.items():
            metrics['tool_metrics'][tool_name] = {
                'count': data['count'],
                'total_time': data['total_time'],
                'avg_duration': data['total_time'] / data['count'] if data['count'] > 0 else 0,
                'errors': data['errors'],
                'last_error': data['last_error'],
                'error_rate': data['errors'] / data['count'] if data['count'] > 0 else 0
            }

        # Process chain metrics
        for chain_name, data in self.chain_metrics.items():
            metrics['chain_metrics'][chain_name] = {
                'count': data['count'],
                'total_time': data['total_time'],
                'avg_duration': data['total_time'] / data['count'] if data['count'] > 0 else 0,
                'errors': data['errors'],
                'last_error': data['last_error'],
                'error_rate': data['errors'] / data['count'] if data['count'] > 0 else 0
            }

        return metrics


def get_skills(agent: db.Agents) -> List:
    """ Retrieve skills from agent `skills` attribute. Specific to agent endpoints. """
    return [rel.skill.type for rel in agent.skills_relationships]
