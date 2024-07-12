from pydantic import BaseModel
from typing import Text, List, Optional


class AmazonBedrockTitanTextConfig(BaseModel):
    """
    Configuration model for invoking Amazon Titan Text models.
    The official documentation can be found at https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-titan-text.html.

    Attributes
    ----------
    temperature : Optional[float]
        Use a lower value to decrease randomness in responses. Maps to `temperature` in the Amazon Bedrock API.

    top_p : Optional[float]
        Use a lower value to ignore less probable options and decrease the diversity of responses. Maps `topP` in the Amazon Bedrock API.

    max_tokens : Optional[int]
        Maximum number of tokens to generate in the response. Maximum token limits are strictly enforced. Maps to `maxTokenCount` in the Amazon Bedrock API.

    stop : Optional[List[Text]]
        Specify a character sequence to indicate where the model should stop. Maps to `stopSequences` in the Amazon Bedrock API.
    """
    temperature: Optional[float]
    top_p: Optional[float]
    max_tokens: Optional[int]
    stop: Optional[List[Text]]

    # TODO: Add validations for the attributes.