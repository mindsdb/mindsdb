from pydantic_settings import BaseSettings
from botocore.exceptions import ClientError
from typing import Text, List, Optional, Any
from pydantic import BaseModel, model_validator, field_validator

from mindsdb.interfaces.storage.model_fs import HandlerStorage

from mindsdb.integrations.handlers.bedrock_handler.utilities import create_amazon_bedrock_client
from mindsdb.integrations.utilities.handlers.validation_utilities import ParameterValidationUtilities


class AmazonBedrockEngineSettings(BaseSettings):
    """
    Settings for Amazon Bedrock handler.

    Attributes
    ----------

    DEFAULT_MODE : Text
        The default mode for the handler.

    SUPPORTED_MODES : List
        List of supported modes for the handler.
    """
    DEFAULT_MODE: Text = 'default'
    SUPPORTED_MODES: List = ['default']


class AmazonBedrockEngineConfig(BaseModel):
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
            bedrock_client.list_foundational_models()
        except ClientError as e:
            raise ValueError(f"Invalid Amazon Bedrock credentials: {e}!")
        

class AmazonBedrockModelConfig(BaseModel):
    """
    Configuration model for Amazon Bedrock models.

    Attributes
    ----------
    model_id: Text
        Amazon Bedrock model ID.

        
    engine: HandlerStorage
        The handler storage from the engine of the model. This is not provided by the user. It is used for validating the model ID.
    """
    model_id: Text
    mode: Optional[Text] = AmazonBedrockEngineSettings.DEFAULT_MODE
    prompt_template: Optional[Text]
    question_column: Optional[Text]

    engine: HandlerStorage

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
    
    @model_validator(mode="before")
    @classmethod
    def check_model_id_is_valid(cls, values: Any) -> Any:
        """
        Validator to check if the model ID provided is valid.

        Args:
            values (Any): Model configuration.

        Raises:
            ValueError: If the model ID is invalid.
        """
        connection_args = values["engine"].get_connection_args()

        bedrock_client = create_amazon_bedrock_client(
            connection_args["aws_access_key_id"],
            connection_args["aws_secret_access_key"],
            connection_args["region_name"],
            connection_args["aws_session_token"]
        )

        try:
            bedrock_client.get_foundational_model(values["model_id"])
        except ClientError as e:
            raise ValueError(f"Invalid Amazon Bedrock model ID: {e}!")
        
        return values
    
    @field_validator("mode", pre=True)
    @classmethod
    def check_mode_is_supported(cls, mode: Text) -> Text:
        """
        Validator to check if the mode provided is supported.

        Args:
            mode (Text): Mode.

        Raises:
            ValueError: If the mode provided is not supported.
        """
        if mode not in AmazonBedrockEngineSettings.SUPPORTED_MODES:
            raise ValueError(f"Mode {mode} is not supported. The supported modes are {" ".join(AmazonBedrockEngineSettings.SUPPORTED_MODES)}!")
        
        return mode
    
    @model_validator(mode="before")
    @classmethod
    def check_params_for_mode_provided(cls, values: Any) -> Any:
        """
        Validator to check if the parameters required for the chosen mode provided are valid.

        Args:
            values (Any): Model configuration.

        Raises:
            ValueError: If the parameters provided are invalid for the mode provided.
        """
        # If the mode is default, one of the following need to be provided:
        # 1. prompt_template
        # 2. question_column
        # TODO: Find the other possible parameters/combinations for the default mode.
        if values["mode"] == "default":
            if "prompt_template" not in values and "question_column" not in values:
                raise ValueError("One of the following parameters need to be provided for the default mode: prompt_template, question_column!")

        # TODO: Add validations for other modes.

        return values

