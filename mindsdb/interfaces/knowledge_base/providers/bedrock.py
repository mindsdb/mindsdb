import json
from typing import List, Optional


def prepare_conversation(messages):
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
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        try:
            import aioboto3  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment specific
            raise ImportError(
                "aioboto3 is required for the Bedrock reranker client. Install it with `pip install aioboto3`."
            ) from exc

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.region_name = region_name

        self.session = aioboto3.Session()
        self._client = None

    async def acompletion(
        self,
        model_name: str,
        messages: List[dict],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        top_p: Optional[float] = None,
    ):
        inferenceConfig = {}
        if temperature:
            inferenceConfig["temperature"] = temperature
        if max_tokens:
            inferenceConfig["max_tokens"] = max_tokens
        if top_p:
            inferenceConfig["top_p"] = top_p

        # convert messages
        conversation = prepare_conversation(messages)

        async with self.session.client(
            "bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            region_name=self.region_name,
        ) as client:
            response = await client.converse(modelId=model_name, messages=conversation, inferenceConfig=inferenceConfig)

        return response["output"]["message"]["content"][0]["text"]


class BedrockClient:
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        try:
            import boto3  # type: ignore
        except ImportError as exc:  # pragma: no cover - environment specific
            raise ImportError("boto3 is required for the Bedrock client. Install it with `pip install boto3`.") from exc

        self.client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
        )

    def embeddings(self, model_name: str, messages: List[str]):
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
    ):
        inferenceConfig = {}
        if temperature:
            inferenceConfig["temperature"] = temperature
        if max_tokens:
            inferenceConfig["max_tokens"] = max_tokens
        if top_p:
            inferenceConfig["top_p"] = top_p

        # convert messages
        conversation = prepare_conversation(messages)

        response = self.client.converse(modelId=model_name, messages=conversation, inferenceConfig=inferenceConfig)

        return response["output"]["message"]["content"][0]["text"]
