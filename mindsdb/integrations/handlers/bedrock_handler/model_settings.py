import sys
import inspect
from pydantic import BaseModel
from typing import Text, List, Optional, ClassVar


def get_config_for_model(provider: Text, output_modalities: List[Text]):
    """
    Get the configuration model for the specified provider and output modalities.

    Args:
        provider (Text): The provider of the model.
        output_modalities (List[Text]): The output modalities of the model.

    Returns:
        BaseModel: The configuration model for the specified provider and output modalities.
    """
    for _, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and hasattr(obj, 'provider') and hasattr(obj, 'output_modalities'):
            if obj.provider == provider and set(obj.output_modalities) == set(output_modalities):
                return obj
    return None


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
    provider: ClassVar = 'Amazon'
    output_modalities: ClassVar = ['TEXT']

    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    stop: Optional[List[Text]] = None

    # TODO: Add validations for the attributes.

    def __dict__(self):
        text_generation_config = {}

        if self.temperature:
            text_generation_config['temperature'] = self.temperature

        if self.top_p:
            text_generation_config['topP'] = self.top_p

        if self.max_tokens:
            text_generation_config['maxTokenCount'] = self.max_tokens

        if self.stop:
            text_generation_config['stopSequences'] = self.stop

        if not text_generation_config:
            return None
        
        return {
            'parameters': {
                'textGenerationConfig': text_generation_config
            }
        }
