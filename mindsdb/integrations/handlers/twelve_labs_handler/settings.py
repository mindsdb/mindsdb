from typing import List, Optional

from pydantic import BaseModel, Extra, root_validator

from mindsdb.integrations.handlers.utilities.validation_utilities import ParameterValidationUtilities


class TwelveLabsHandlerConfig(BaseModel):
    "Configuration for TwelveLabsHandler."

    index_name: str
    engine_id: Optional[str] = None
    api_key: Optional[str] = None
    index_options: List[str]
    addons: List[str] = []
    video_urls: Optional[List[str]] = None
    video_urls_col: Optional[str] = None
    video_files: Optional[List[str]] = None
    video_files_col: Optional[str] = None
    task: str = None
    search_options: Optional[List[str]] = None

    class Config:
        extra = Extra.forbid

    @root_validator(pre=True, allow_reuse=True)
    def check_param_typos(cls, values):
        """Check if there are any typos in the parameters."""

        ParameterValidationUtilities.validate_parameter_spelling(cls, values)

    @root_validator(allow_reuse=True, skip_on_failure=True)
    def check_for_video_urls_or_video_files(cls, values):
        """Check if video_urls or video_files have been provided."""

        video_urls = values.get("video_urls")
        video_urls_col = values.get("video_urls_col")
        video_files = values.get("video_files")
        video_files_col = values.get("video_files_col")

        if not video_urls and not video_files and not video_urls_col and not video_files_col:
            raise ValueError(
                "Neither video_urls, video_files, video_urls_col nor video_files_col have been provided. Please provide one of them."
            )

        return values

    @root_validator(allow_reuse=True, skip_on_failure=True)
    def check_for_task(cls, values):
        """Check if task has been provided along with the other relevant parameters for each task."""

        task = values.get("task")

        if task == "search":
            search_options = values.get("search_options")
            if not search_options:
                raise ValueError(
                    "search_options have not been provided. Please provide search_options."
                )

            # search options should be a subset of index options
            index_options = values.get("index_options")
            if not set(search_options).issubset(set(index_options)):
                raise ValueError(
                    "search_options should be a subset of index_options."
                )

        else:
            raise ValueError(
                f"task {task} is not supported. Please provide a valid task."
            )

        return values
