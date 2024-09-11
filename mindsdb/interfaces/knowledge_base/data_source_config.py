from enum import Enum
from typing import Union, List
from uuid import uuid4
from datetime import datetime

from pydantic import BaseModel, Field


class DataSourceConfig(BaseModel):
    """
    Represents a data source that can be made available to a Mind.
    """
    id: str = Field(default_factory=lambda: uuid4().hex)

    # Description for underlying agent to know, based on context, whether to access this data source.
    description: str



class S3Config(DataSourceConfig):
    """
    A configuration in order to find and filter a collection of files in an AWS Bucket.

    :param buckets: A list of bucket names or regexes to match bucket names to.
    :param files: A list of filenames or regexes to match filenames to.
    :param aws_access_key_id: Access Key for AWS, defaults to boto defaults (e.g. viewing .aws/credentials).
    :param aws_secret_access_key: Secret Key for AWS, defaults to boto defaults (e.g. viewing .aws/credentials).
    :param region_name: Region for AWS, defaults to boto defaults (e.g. viewing .aws/credentials).
    :param aws_session_token: AWS session token for temporary credentials.
    :param update_from_last: Optional datetime to filter files by their last modified date; only files modified after this date will be considered.
    """

    buckets: List[str] = []
    files: List[str] = []
    aws_access_key_id: Union[str, None] = None
    aws_secret_access_key: Union[str, None] = None
    region_name: Union[str, None] = None
    aws_session_token: Union[str, None] = None
    update_from_last: Union[datetime, None] = None  # New optional field for filtering files by date