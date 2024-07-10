from typing import Any, Dict, Union
from uuid import uuid4

from langchain_core.callbacks.base import BaseCallbackHandler

from mindsdb.utilities import log


logger = log.getLogger(__name__)
logger.setLevel('DEBUG')

class LangfuseCallbackHandler(BaseCallbackHandler):
    '''Langchain callback handler that traces tool & chain executions using Langfuse.'''
    def __init__(self, langfuse, trace_id: str, observation_id: str):
        self.langfuse = langfuse
        self.trace_id = trace_id
        self.observation_id = observation_id
        self.tool_uuid_to_span = {}
        self.chain_uuid_to_span = {}

    def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> Any:
        '''Run when tool starts running.'''
        run_uuid = kwargs.get('run_id', uuid4()).hex
        tool_span = self.langfuse.span(
            name=f'{serialized.get("name", "")}-{run_uuid}',
            trace_id=self.trace_id,
            parent_observation_id=self.observation_id,
            input=input_str
        )
        self.tool_uuid_to_span[run_uuid] = tool_span

    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        '''Run when tool ends running.'''
        run_uuid = kwargs.get('run_id', uuid4()).hex
        if run_uuid not in self.tool_uuid_to_span:
            return
        tool_span = self.tool_uuid_to_span.pop(run_uuid)
        tool_span.update(output=output)
        tool_span.end()

    def on_tool_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        '''Run when tool errors.'''
        # Do nothing for now.
        pass

    def on_chain_start(
        self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any
    ) -> Any:
        '''Run when chain starts running.'''
        run_uuid = kwargs.get('run_id', uuid4()).hex
        chain_span = self.langfuse.span(
            name=f'{serialized.get("name", "chain")}-{run_uuid}',
            trace_id=self.trace_id,
            parent_observation_id=self.observation_id,
            input=str(inputs)
        )
        self.chain_uuid_to_span[run_uuid] = chain_span

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> Any:
        '''Run when chain ends running.'''
        chain_uuid = kwargs.get('run_id', uuid4()).hex
        if chain_uuid not in self.chain_uuid_to_span:
            return
        chain_span = self.chain_uuid_to_span.pop(chain_uuid)
        chain_span.update(output=str(outputs))
        chain_span.end()

    def on_chain_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        '''Run when chain errors.'''
        # Do nothing for now.
        pass
