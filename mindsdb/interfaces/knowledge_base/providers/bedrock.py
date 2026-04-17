import json
from typing import Dict, List, Optional


def prepare_conversation(messages: List[dict]) -> List[dict]:
    """Convert chat messages to Bedrock `converse` message payload format."""
    conversation = []
    for message in messages:
        content = message["content"]
        role = message["role"]
        if role == "system":
            role = "assistant"
        if role != "user":
            if len(conversation) == 0:
                # the first message has to be user message
                content = message["role"] + ":\n" + content
                role = "user"

        conversation.append(
            {
                "role": role,
                "content": [{"text": content}],
            }
        )
    return conversation


class AsyncBedrockClient:
    """Async Bedrock runtime client wrapper"""

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region_name: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        try:
            from aiobotocore.session import get_session
        except ImportError as exc:
            raise ImportError(
                "aiobotocore is required for the Bedrock reranker client. Install it with `pip install aiobotocore`."
            ) from exc

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.region_name = aws_region_name

        self._session = get_session()

    async def acompletion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """Generate a chat completion asynchronously via Bedrock."""
        inference_config = {}
        if temperature is not None:
            inference_config["temperature"] = temperature
        if max_tokens is not None:
            inference_config["max_tokens"] = max_tokens
        if top_p is not None:
            inference_config["top_p"] = top_p

        conversation = prepare_conversation(messages)

        # Create client with credentials
        client_kwargs = {
            "service_name": "bedrock-runtime",
            "region_name": self.region_name,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_session_token": self.aws_session_token,
        }

        async with self._session.create_client(**client_kwargs) as client:
            response = await client.converse(
                modelId=model_name, messages=conversation, inferenceConfig=inference_config
            )

        return response["output"]["message"]["content"][0]["text"]


class BedrockClient:
    """Synchronous Bedrock runtime client wrapper"""

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region_name: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        try:
            import boto3
        except ImportError as exc:
            raise ImportError("boto3 is required for the Bedrock client. Install it with `pip install boto3`.") from exc

        self.client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region_name,
        )

    def embeddings(self, model_name: str, messages: List[str]) -> List[List[float]]:
        """Request embedding vectors for each text in `messages`."""
        embeddings = []
        for message in messages:
            native_request = {"inputText": message}
            request = json.dumps(native_request)

            response = self.client.invoke_model(modelId=model_name, body=request)
            model_response = json.loads(response["body"].read())

            # Extract and print the generated embedding and the input text token count.
            embeddings.append(model_response["embedding"])

        return embeddings

    def completion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """Generate a chat completion synchronously via Bedrock."""
        inference_config: Dict[str, float | int] = {}
        if temperature is not None:
            inference_config["temperature"] = temperature
        if max_tokens is not None:
            inference_config["max_tokens"] = max_tokens
        if top_p is not None:
            inference_config["top_p"] = top_p

        conversation = prepare_conversation(messages)

        response = self.client.converse(modelId=model_name, messages=conversation, inferenceConfig=inference_config)

        return response["output"]["message"]["content"][0]["text"]
