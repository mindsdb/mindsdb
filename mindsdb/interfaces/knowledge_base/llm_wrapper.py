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
        # Process each prompt separately since completion expects a single list of messages
        responses = []
        for prompt in prompts:
            # Convert prompt to messages format expected by LLMClient
            messages = [{"role": "user", "content": prompt}]
            
            # Call completion for this prompt's messages
            result = self.llm_client.completion(messages)
            
            # completion returns a list, get the first (and only) response
            if result:
                responses.append(LLMResponse(content=result[0]))
            else:
                responses.append(LLMResponse(content=""))
        
        return responses

    async def abatch(self, prompts: List[str]) -> List[LLMResponse]:
        """
        Process a batch of prompts asynchronously

        Args:
            prompts: List of prompt strings

        Returns:
            List of LLMResponse objects with content attribute
        """
        # Process each prompt separately since completion expects a single list of messages
        loop = asyncio.get_event_loop()
        responses = []
        
        for prompt in prompts:
            # Convert prompt to messages format expected by LLMClient
            messages = [{"role": "user", "content": prompt}]
            
            # Run completion in executor for async compatibility
            result = await loop.run_in_executor(
                None,
                self.llm_client.completion,
                messages
            )
            
            # completion returns a list, get the first (and only) response
            if result:
                responses.append(LLMResponse(content=result[0]))
            else:
                responses.append(LLMResponse(content=""))
        
        return responses


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

