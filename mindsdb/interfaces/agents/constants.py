from langchain.agents import AgentType

DEFAULT_AGENT_TIMEOUT_SECONDS = 300
# These should require no additional arguments.
DEFAULT_AGENT_TOOLS = []
DEFAULT_AGENT_TYPE = AgentType.CONVERSATIONAL_REACT_DESCRIPTION
DEFAULT_MAX_ITERATIONS = 10
DEFAULT_MAX_TOKENS = 2048
DEFAULT_MODEL_NAME = 'gpt-4-0125-preview'

USER_COLUMN = 'question'
ASSISTANT_COLUMN = 'answer'


SUPPORTED_PROVIDERS = {'openai', 'anthropic', 'anyscale', 'litellm', 'ollama'}
