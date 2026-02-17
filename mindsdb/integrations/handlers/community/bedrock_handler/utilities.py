import boto3
from typing import Text, Optional


def create_amazon_bedrock_client(
    client: Text,
    aws_access_key_id: Text,
    aws_secret_access_key: Text,
    region_name: Text,
    aws_session_token: Optional[Text] = None,
) -> boto3.client:
    """
    Create an Amazon Bedrock client via boto3.

    Parameters
    ----------
    client : Text
        The type of client to create. It can be 'bedrock' or 'bedrock-runtime'.

    aws_access_key_id : Text
        The AWS access key ID.

    aws_secret_access_key : Text
        The AWS secret access key.

    region_name : Text
        The AWS region name.

    aws_session_token : Text, Optional
        The AWS session token. Optional, but required for temporary security credentials.

    Returns
    -------
    boto3.client
        Amazon Bedrock client.
    """
    if client not in ["bedrock", "bedrock-runtime"]:
        raise ValueError("The client must be 'bedrock' or 'bedrock-runtime'")

    return boto3.client(
        client,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        aws_session_token=aws_session_token,
    )
