import re
from typing import Union

from langchain_core.agents import AgentAction, AgentFinish

from langchain.agents.agent import AgentOutputParser
from langchain.agents.conversational.prompt import FORMAT_INSTRUCTIONS

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SafeOutputParser(AgentOutputParser):
    '''Output parser for the conversational agent that does not throw OutputParserException.'''

    ai_prefix: str = 'AI'
    '''Prefix to use before AI output.'''

    format_instructions: str = FORMAT_INSTRUCTIONS
    '''Default formatting instructions'''

    def get_format_instructions(self) -> str:
        '''Returns formatting instructions for the given output parser.'''
        return self.format_instructions

    def parse(self, text: str) -> Union[AgentAction, AgentFinish]:
        '''Parses outputted text from an LLM.

        Args:
            text (str): Outputted text to parse.

        Returns:
            output (str): Parsed text to an Agent step.
        '''
        regex = r'Action: (.*?)[\n]*Action Input:([\s\S]*)'
        match = re.search(regex, text, re.DOTALL)
        if match is not None:
            action = match.group(1)
            action_input = match.group(2)
            return AgentAction(action.strip(), action_input.strip(' ').strip('"'), text)
        output = text
        if f'{self.ai_prefix}:' in text:
            output = text.split(f'{self.ai_prefix}:')[-1].strip()
        return AgentFinish(
            {'output': output}, text
        )

    @property
    def _type(self) -> str:
        return 'conversational'
