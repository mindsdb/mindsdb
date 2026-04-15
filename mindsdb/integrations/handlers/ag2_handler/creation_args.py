from collections import OrderedDict

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

creation_args = OrderedDict(
    openai_api_key={
        "type": ARG_TYPE.STR,
        "description": "OpenAI API key for the agents LLM backend. Falls back to OPENAI_API_KEY env var.",
        "required": False,
        "label": "OpenAI API key",
        "secret": True,
    },
    model={
        "type": ARG_TYPE.STR,
        "description": "LLM model name (default: gpt-4o-mini).",
        "required": False,
        "label": "Model name",
    },
    api_type={
        "type": ARG_TYPE.STR,
        "description": "LLM API type: openai, anthropic, bedrock, etc. (default: openai).",
        "required": False,
        "label": "API type",
    },
    api_base={
        "type": ARG_TYPE.STR,
        "description": "Custom API base URL for OpenAI-compatible endpoints.",
        "required": False,
        "label": "API base URL",
    },
)
