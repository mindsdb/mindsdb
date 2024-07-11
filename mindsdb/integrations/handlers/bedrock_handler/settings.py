from typing import Text, Optional, Any
from botocore.exceptions import ClientError
from pydantic import BaseModel, model_validator

from mindsdb.integrations.handlers.bedrock_handler.utilities import create_amazon_bedrock_client
from mindsdb.integrations.utilities.handlers.validation_utilities import ParameterValidationUtilities


class AmazonBedrockEngineConfig(BaseModel):
    """
    Configuration model for Amazon Bedrock engines.

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
    def check_param_typos(cls, values: Any) -> Any:
        """
        Root validator to check if there are any typos in the parameters.

        Parameters
        ----------
        values : Dict
            Dictionary containing the attributes of the model.

        Raises
        ------
        ValueError
            If there are any typos in the parameters.
        """
        ParameterValidationUtilities.validate_parameter_spelling(cls, values)

        return values
    
    @model_validator(mode="after")
    @classmethod
    def check_access_to_amazon_bedrock(cls, model: BaseModel) -> BaseModel:
        """
        Root validator to check if the Amazon Bedrock credentials are valid and Amazon Bedrock is accessible.

        Parameters
        ----------
        model : BaseModel
            Model containing the attributes of the engine.

        Raises
        ------
        ValueError
            If the Amazon Bedrock credentials are invalid.
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
            raise ValueError(f"Invalid Amazon Bedrock credentials: {e}")