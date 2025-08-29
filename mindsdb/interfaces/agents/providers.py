import importlib.util


def get_bedrock_chat_model():
    try:
        from langchain_aws.chat_models import ChatBedrock
    except ModuleNotFoundError:
        raise RuntimeError("bedrock connector is not installed. Please install it with `pip install langchain-aws`")

    if not importlib.util.find_spec("transformers"):
        raise RuntimeError(
            "`transformers` module is required for bedrock to count tokens. Please install it with `pip install transformers`"
        )

    class ChatBedrockPatched(ChatBedrock):
        def _prepare_input_and_invoke(self, *args, **kwargs):
            kwargs.pop("stop_sequences", None)
            return super()._prepare_input_and_invoke(*args, **kwargs)

    return ChatBedrockPatched
