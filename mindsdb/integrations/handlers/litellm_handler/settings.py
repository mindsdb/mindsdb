from typing import List, Optional, Union, Dict
from pydantic import BaseModel, Extra


class CompletionParameters(BaseModel):
    model: str  # The model to be used for the API call.
    prompt_template: Optional[str] = None  # Template for the prompt to be used.

    # Optional OpenAI params: see  https://platform.openai.com/docs/api-reference/chat/runs
    tool_choice: Optional[str] = None  # Specific tool to be used, if applicable.

    timeout: Optional[Union[float, int]] = None  # Timeout for the API request.
    temperature: Optional[float] = None  # Controls randomness: higher value means more random responses.
    top_p: Optional[float] = None  # Nucleus sampling: higher value means more diverse responses.
    n: Optional[int] = None  # Number of completions to generate for each prompt.
    stream: Optional[bool] = None  # Whether to stream responses or not.
    stop: Optional[str] = None  # Sequence at which the model should stop generating further tokens.
    max_tokens: Optional[float] = None  # Maximum number of tokens to generate in each completion.
    presence_penalty: Optional[float] = None  # Penalty for new tokens based on their existing presence in the text.
    frequency_penalty: Optional[float] = None  # Penalty for new tokens based on their frequency in the text.
    logit_bias: Optional[Dict] = None  # Adjusts the likelihood of specified tokens appearing in the completion.
    user: Optional[str] = None  # Identifier for the end-user, for usage tracking.

    # openai v1.0+ new params
    response_format: Optional[Dict] = None  # Format of the response, if specific formatting is required.
    seed: Optional[int] = None  # Random seed for deterministic completions.
    tools: Optional[List[str]] = None  # List of additional tools or features to be used.
    tool_choice: Optional[str] = None  # Specific tool to be used, if applicable.
    deployment_id: Optional[str] = None  # Identifier for the deployment.

    # set api_base, api_version, api_key
    base_url: Optional[str] = None  # Base URL of the API.
    api_version: Optional[str] = None  # Version of the API to be used.
    api_key: Optional[str] = None  # API key for authentication.
    target: Optional[str] = None  # the name of output column

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True
        use_enum_values = True
