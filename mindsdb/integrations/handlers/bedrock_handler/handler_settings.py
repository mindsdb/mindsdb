from pydantic_settings import BaseSettings
from botocore.exceptions import ClientError
from typing import Text, List, Dict, Optional, Any, ClassVar
from pydantic import BaseModel, Field, model_validator, field_validator

from mindsdb.integrations.handlers.bedrock_handler.utilities import create_amazon_bedrock_client
from mindsdb.integrations.utilities.handlers.validation_utilities import ParameterValidationUtilities


class AmazonBedrockHandlerSettings(BaseSettings):
    """
    Settings for Amazon Bedrock handler.

    Attributes
    ----------

    DEFAULT_MODE : Text
        The default mode for the handler.

    SUPPORTED_MODES : List
        List of supported modes for the handler.
    """
    # TODO: Add other modes.
    DEFAULT_MODE: ClassVar[Text] = 'default'
    SUPPORTED_MODES: ClassVar[List] = ['default']


class AmazonBedrockHandlerEngineConfig(BaseModel):
    """
    Model for Amazon Bedrock engines.

    Attributes
    ----------
    aws_access_key_id : Text
        AWS access key ID.

    aws_secret_access_key : Text
        AWS secret access key.

    region_name : Text
        AWS region name.

    aws_session_token : Text, Optional
        AWS session token. Optional, but required for temporary security credentials.
    """
    aws_access_key_id: Text
    aws_secret_access_key: Text
    region_name: Text
    aws_session_token: Optional[Text]

    class Config:
        extra = "forbid"

    @model_validator(mode="before")
    @classmethod
    def check_params_contain_typos(cls, values: Any) -> Any:
        """
        Validator to check if there are any typos in the parameters.

        Args:
            values (Any): Engine configuration.

        Raises:
            ValueError: If there are any typos in the parameters.
        """
        ParameterValidationUtilities.validate_parameter_spelling(cls, values)

        return values
    
    @model_validator(mode="after")
    @classmethod
    def check_access_to_amazon_bedrock(cls, model: BaseModel) -> BaseModel:
        """
        Validator to check if the Amazon Bedrock credentials are valid and Amazon Bedrock is accessible.

        Args:
            model (BaseModel): Engine configuration.

        Raises:
            ValueError: If the AWS credentials are invalid or do not have access to Amazon Bedrock.
        """
        bedrock_client = create_amazon_bedrock_client(
            model.aws_access_key_id,
            model.aws_secret_access_key,
            model.region_name,
            model.aws_session_token
        )

        try:
            bedrock_client.list_foundation_models()
        except ClientError as e:
            raise ValueError(f"Invalid Amazon Bedrock credentials: {e}!")
        
        return model
        

class AmazonBedrockHandlerModelConfig(BaseModel):
    """
    Configuration model for Amazon Bedrock models.

    Attributes
    ----------

    """
    # User-provided Handler Model Prameters: These are parameters specific to the MindsDB handler for Amazon Bedrock provided by the user.
    model_id: Text = Field(...)
    mode: Optional[Text] = Field(AmazonBedrockHandlerSettings.DEFAULT_MODE)
    prompt_template: Optional[Text] = Field(None)
    question_column: Optional[Text] = Field(None)

    # Amazon Bedrock Model Parameters: These are parameters specific to the models in Amazon Bedrock. They are provided by the user.
    temperature: Optional[float] = Field(None, bedrock_model_param=True)
    top_p: Optional[float] = Field(None, bedrock_model_param=True)
    max_tokens: Optional[int] = Field(None, bedrock_model_param=True)
    stop: Optional[List[Text]] = Field(None, bedrock_model_param=True)

    # System-provided Handler Model Parameters: These are parameters specific to the MindsDB handler for Amazon Bedrock provided by the system.
    connection_args: Dict = Field(None, exclude=True)

    class Config:
        extra = "forbid"

    @model_validator(mode="before")
    @classmethod
    def check_params_contain_typos(cls, values: Any) -> Any:
        """
        Validator to check if there are any typos in the parameters.

        Args:
            values (Any): Model configuration.

        Raises:
            ValueError: If there are any typos in the parameters.
        """
        ParameterValidationUtilities.validate_parameter_spelling(cls, values)

        return values
    
    @model_validator(mode="after")
    @classmethod
    def check_model_id_and_params_are_valid(cls, model: BaseModel) -> BaseModel:
        """
        Validator to check if the model ID and the parameters provided for the model are valid.

        Args:
            values (Any): Model configuration.

        Raises:
            ValueError: If the model ID provided is invalid or the parameters provided are invalid for the chosen model.
        """
        bedrock_client = create_amazon_bedrock_client(
            **model.connection_args
        )

        try:
            bedrock_client.get_foundation_model(modelIdentifier=model.model_id)
        except ClientError as e:
            raise ValueError(f"Invalid Amazon Bedrock model ID: {e}!")
        
        return model
    
    @field_validator("mode")
    @classmethod
    def check_mode_is_supported(cls, mode: Text) -> Text:
        """
        Validator to check if the mode provided is supported.

        Args:
            mode (Text): The mode to run the handler model in.

        Raises:
            ValueError: If the mode provided is not supported.
        """
        if mode not in AmazonBedrockHandlerSettings.SUPPORTED_MODES:
            raise ValueError(f"Mode {mode} is not supported. The supported modes are {''.join(AmazonBedrockHandlerSettings.SUPPORTED_MODES)}!")
        
        return mode
    
    @model_validator(mode="after")
    @classmethod
    def check_mode_params_provided(cls, model: BaseModel) -> BaseModel:
        """
        Validator to check if the parameters required for the chosen mode provided are valid.

        Args:
            model (BaseModel): Handler model configuration.

        Raises:
            ValueError: If the parameters provided are invalid for the mode provided.
        """
        # If the mode is default, one of the following need to be provided:
        # 1. prompt_template
        # 2. question_column
        # TODO: Find the other possible parameters/combinations for the default mode.
        if model.mode == "default":
            if not model.prompt_template and not model.question_column:
                raise ValueError("One of the following parameters need to be provided for the default mode: prompt_template, question_column!")

        # TODO: Add validations for other modes.

        return model

    def model_dump(self) -> Dict:
        """
        Dumps the model configuration to a dictionary.

        Returns:
            Dict: The model configuration.
        """
        bedrock_model_params = [key for key, val in self.model_json_schema(mode='serialization')['properties'].items() if val.get("bedrock_model_param")]
        handler_model_params = [key for key, val in self.model_json_schema(mode='serialization')['properties'].items() if not val.get("bedrock_model_param")]

        return {
            "inference_config": {key: getattr(self, key) for key in bedrock_model_params if getattr(self, key) is not None},
            **{key: getattr(self, key) for key in handler_model_params}
        }