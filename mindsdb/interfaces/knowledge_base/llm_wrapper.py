"""Custom LLM wrapper to replace langchain create_chat_model"""

import asyncio
from typing import List, Any, Dict
from dataclasses import dataclass

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient
from mindsdb.utilities import log

logger = log.getLogger(__name__)


@dataclass
class LLMResponse:
    """Simple response object with content attribute to match langchain interface"""
    content: str


class CustomLLMWrapper:
    """
    Custom LLM wrapper that wraps LLMClient to provide langchain-compatible interface.
    This replaces langchain's create_chat_model for use in knowledge_base.
    """

    def __init__(self, args: Dict[str, Any], session=None):
        """
        Initialize the LLM wrapper

        Args:
            args: Dictionary with model_name, provider, and other LLM parameters
            session: Optional session for LLMClient
        """
        # Prepare params for LLMClient
        params = {
            "model_name": args.get("model_name"),
            "provider": args.get("provider", "openai"),
            **{k: v for k, v in args.items() if k not in ["model_name", "provider"]}
        }
        
        self.llm_client = LLMClient(params=params, session=session)

    def batch(self, prompts: List[str]) -> List[LLMResponse]:
        """
        Process a batch of prompts synchronously

        Args:
            prompts: List of prompt strings

        Returns:
            List of LLMResponse objects with content attribute
        """
        # Convert prompts to messages format expected by LLMClient
        messages_list = [[{"role": "user", "content": prompt}] for prompt in prompts]
        
        # Call completion for all messages
        responses = self.llm_client.completion(messages_list)
        
        # Convert to LLMResponse objects
        return [LLMResponse(content=resp) for resp in responses]

    async def abatch(self, prompts: List[str]) -> List[LLMResponse]:
        """
        Process a batch of prompts asynchronously

        Args:
            prompts: List of prompt strings

        Returns:
            List of LLMResponse objects with content attribute
        """
        # Convert prompts to messages format
        messages_list = [[{"role": "user", "content": prompt}] for prompt in prompts]
        
        # Run completion in executor for async compatibility
        loop = asyncio.get_event_loop()
        responses = await loop.run_in_executor(
            None,
            self.llm_client.completion,
            messages_list
        )
        
        # Convert to LLMResponse objects
        return [LLMResponse(content=resp) for resp in responses]


def create_chat_model(args: Dict[str, Any], session=None) -> CustomLLMWrapper:
    """
    Create a custom LLM wrapper (replacement for langchain's create_chat_model)

    Args:
        args: Dictionary with model_name, provider, and other LLM parameters
        session: Optional session for LLMClient

    Returns:
        CustomLLMWrapper instance
    """
    return CustomLLMWrapper(args, session=session)

