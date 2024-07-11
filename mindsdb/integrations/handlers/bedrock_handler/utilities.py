import boto3
from typing import Text, Optional, Any


def create_amazon_bedrock_client(
    aws_access_key_id: Text,
    aws_secret_access_key: Text,
    region_name: Text,
    aws_session_token: Optional[Text] = None,
) -> boto3.client:
    """
    Create an Amazon Bedrock client via boto3.

    Parameters
    ----------
    aws_access_key_id : Text
        AWS access key ID.

    aws_secret_access_key : Text
        AWS secret access key.

    region_name : Text
        AWS region name.

    aws_session_token : Text, Optional
        AWS session token. Optional, but required for temporary security credentials.

    Returns
    -------
    boto3.client
        Amazon Bedrock client.
    """
    return boto3.client(
        "bedrock",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        aws_session_token=aws_session_token,
    )


def create_amazon_bedrock_runtime_client(
    aws_access_key_id: Text,
    aws_secret_access_key: Text,
    region_name: Text,
    aws_session_token: Optional[Text] = None,
) -> boto3.client:
    """
    Create an Amazon Bedrock runtime client via boto3.

    Parameters
    ----------
    aws_access_key_id : Text
        AWS access key ID.

    aws_secret_access_key : Text
        AWS secret access key.

    region_name : Text
        AWS region name.

    aws_session_token : Text, Optional
        AWS session token. Optional, but required for temporary security credentials.

    Returns
    -------
    boto3.client
        Amazon Bedrock runtime client.
    """
    return boto3.client(
        "bedrock-runtime",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        aws_session_token=aws_session_token,
    )