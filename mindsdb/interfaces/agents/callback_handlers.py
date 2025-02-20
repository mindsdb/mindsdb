import io
import logging
import contextlib
from typing import Any, Dict, List, Union, Callable

from langchain_core.agents import AgentAction, AgentFinish
from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.messages.base import BaseMessage
from langchain_core.outputs import LLMResult
from langchain_core.callbacks import StdOutCallbackHandler


class ContextCaptureCallback(BaseCallbackHandler):
    def __init__(self):
        self.context = None

    def on_retriever_end(self, documents: List[Any], *, run_id: str, parent_run_id: Union[str, None] = None, **kwargs: Any) -> Any:
        self.context = [{
            'page_content': doc.page_content,
            'metadata': doc.metadata
        } for doc in documents]

    def get_contexts(self):
        return self.context


class VerboseLogCallbackHandler(StdOutCallbackHandler):
    def __init__(self, logger: logging.Logger, verbose: bool):
        self.logger = logger
        self.verbose = verbose
        super().__init__()

    def __call(self, method: Callable, *args: List[Any], **kwargs: Any) -> Any:
        if self.verbose is False:
            return
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            method(*args, **kwargs)
        output = f.getvalue()
        self.logger.info(output)

    def on_chain_start(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_chain_start, *args, **kwargs)

    def on_chain_end(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_chain_end, *args, **kwargs)

    def on_agent_action(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_agent_action, *args, **kwargs)

    def on_tool_end(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_tool_end, *args, **kwargs)

    def on_text(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_text, *args, **kwargs)

    def on_agent_finish(self, *args: List[Any], **kwargs: Any) -> None:
        self.__call(super().on_agent_finish, *args, **kwargs)


class LogCallbackHandler(BaseCallbackHandler):
    '''Langchain callback handler that logs agent and chain executions.'''

    def __init__(self, logger: logging.Logger, verbose: bool = True):
        logger.setLevel('DEBUG')
        self.logger = logger
        self._num_running_chains = 0
        self.generated_sql = None
        self.verbose_log_handler = VerboseLogCallbackHandler(logger, verbose)

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> Any:
        '''Run when LLM starts running.'''
        self.logger.debug('LLM started with prompts:')
        for prompt in prompts:
            self.logger.debug(prompt[:50])
        self.verbose_log_handler.on_llm_start(serialized, prompts, **kwargs)

    def on_chat_model_start(
            self,
            serialized: Dict[str, Any],
            messages: List[List[BaseMessage]], **kwargs: Any
    ) -> Any:
        '''Run when Chat Model starts running.'''
        self.logger.debug('Chat model started with messages:')
        for message_list in messages:
            for message in message_list:
                self.logger.debug(message.pretty_repr())

    def on_llm_new_token(self, token: str, **kwargs: Any) -> Any:
        '''Run on new LLM token. Only available when streaming is enabled.'''
        pass

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> Any:
        '''Run when LLM ends running.'''
        self.logger.debug('LLM ended with response:')
        self.logger.debug(str(response.llm_output))

    def on_llm_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        '''Run when LLM errors.'''
        self.logger.debug(f'LLM encountered an error: {str(error)}')

    def on_chain_start(
        self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any
    ) -> Any:
        '''Run when chain starts running.'''
        self._num_running_chains += 1
        self.logger.info('Entering new LLM chain ({} total)'.format(
            self._num_running_chains))
        self.logger.debug('Inputs: {}'.format(inputs))

        self.verbose_log_handler.on_chain_start(serialized=serialized, inputs=inputs, **kwargs)

    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> Any:
        '''Run when chain ends running.'''
        self._num_running_chains -= 1
        self.logger.info('Ended LLM chain ({} total)'.format(
            self._num_running_chains))
        self.logger.debug('Outputs: {}'.format(outputs))

        self.verbose_log_handler.on_chain_end(outputs=outputs, **kwargs)

    def on_chain_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        '''Run when chain errors.'''
        self._num_running_chains -= 1
        self.logger.error(
            'LLM chain encountered an error ({} running): {}'.format(
                self._num_running_chains, error))

    def on_tool_start(
        self, serialized: Dict[str, Any], input_str: str, **kwargs: Any
    ) -> Any:
        '''Run when tool starts running.'''
        pass

    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        '''Run when tool ends running.'''
        self.verbose_log_handler.on_tool_end(output=output, **kwargs)

    def on_tool_error(
        self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any
    ) -> Any:
        '''Run when tool errors.'''
        pass

    def on_text(self, text: str, **kwargs: Any) -> Any:
        '''Run on arbitrary text.'''
        self.verbose_log_handler.on_text(text=text, **kwargs)

    def on_agent_action(self, action: AgentAction, **kwargs: Any) -> Any:
        '''Run on agent action.'''
        self.logger.debug(f'Running tool {action.tool} with input:')
        self.logger.debug(action.tool_input)

        stop_block = 'Observation: '
        if stop_block in action.tool_input:
            action.tool_input = action.tool_input[: action.tool_input.find(stop_block)]

        if action.tool.startswith("sql_db_query"):
            # Save the generated SQL query
            self.generated_sql = action.tool_input

        # fix for mistral
        action.tool = action.tool.replace('\\', '')

        self.verbose_log_handler.on_agent_action(action=action, **kwargs)

    def on_agent_finish(self, finish: AgentFinish, **kwargs: Any) -> Any:
        '''Run on agent end.'''
        self.logger.debug('Agent finished with return values:')
        self.logger.debug(str(finish.return_values))
        self.verbose_log_handler.on_agent_finish(finish=finish, **kwargs)
