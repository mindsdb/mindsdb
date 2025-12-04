import re
from typing import Union
from dataclasses import dataclass

from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Default format instructions for conversational agent
# This is a simplified version - can be customized if needed
FORMAT_INSTRUCTIONS = """Use the following format:

Question: the input question you must answer
Thought: you should think about what to do
Action: the action to take, should be one of the available tools
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question"""


@dataclass
class AgentAction:
    """Custom AgentAction class to replace langchain AgentAction"""
    tool: str
    tool_input: str
    log: str


@dataclass
class AgentFinish:
    """Custom AgentFinish class to replace langchain AgentFinish"""
    return_values: dict
    log: str


class SafeOutputParser:
    '''Output parser for the conversational agent that does not throw OutputParserException.'''

    def __init__(self, ai_prefix: str = 'AI', format_instructions: str = FORMAT_INSTRUCTIONS):
        self.ai_prefix = ai_prefix
        self.format_instructions = format_instructions

    def get_format_instructions(self) -> str:
        '''Returns formatting instructions for the given output parser.'''
        return self.format_instructions

    def parse(self, text: str) -> Union[AgentAction, AgentFinish]:
        '''Parses outputted text from an LLM.

        Args:
            text (str): Outputted text to parse.

        Returns:
            Union[AgentAction, AgentFinish]: Parsed agent action or finish result
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
    
    def extract_output(self, result: Union[AgentAction, AgentFinish, str]) -> str:
        """Extract the actual output text from a parse result.
        
        Args:
            result: Result from parse() method or a string
            
        Returns:
            str: The actual output text
        """
        if isinstance(result, str):
            return result
        elif isinstance(result, AgentFinish):
            return result.return_values.get('output', result.log)
        elif isinstance(result, AgentAction):
            # For AgentAction, return the log or tool_input
            return result.tool_input if result.tool_input else result.log
        else:
            return str(result)

    @property
    def _type(self) -> str:
        return 'conversational'
