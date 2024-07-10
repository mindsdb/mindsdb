from typing import List, Optional, Any

from pydantic import BaseModel, model_validator
from pydantic_settings import BaseSettings

from mindsdb.integrations.utilities.handlers.validation_utilities import ParameterValidationUtilities


class TwelveLabsHandlerModel(BaseModel):
    """
    Model for the Twelve Labs handler.

    Attributes
    ----------

    index_name : str
        Name of the index to be created or used.

    engine_id : str, Optional
        ID of the engine. If not provided, the default engine is used.

    api_key : str, Optional
        API key for the Twelve Labs API. If not provided, attempts will be made to get the API key from the following sources:
            1. From the engine storage.
            2. From the environment variable TWELVE_LABS_API_KEY.
            3. From the config.json file.

    base_url : str, Optional
        Base URL for the Twelve Labs API. If not provided, the default URL https://api.twelvelabs.io/v1.2 is used.

    index_options : List[str]
        List of that specifies how the platform will process the videos uploaded to this index. This will have no effect if the index already exists.

    addons : List[str], Optional
        List of addons that should be enabled for the index. This will have no effect if the index already exists.

    video_urls : List[str], Optional
        List of video URLs to be indexed. Either video_urls, video_files, video_urls_column or video_files_column should be provided.

    video_urls_column : str, Optional
        Name of the column containing video URLs to be indexed. Either video_urls, video_files, video_urls_column or video_files_column should be provided.

    video_files : List[str], Optional
        List of video files to be indexed. Either video_urls, video_files, video_urls_column or video_files_column should be provided.

    video_files_column : str, Optional
        Name of the column containing video files to be indexed. Either video_urls, video_files, video_urls_column or video_files_column should be provided.

    task : str, Optional
        Task to be performed.

    search_options : List[str], Optional
        List of search options to be used for searching. This will only be required if the task is search.

    search_query_column : str, Optional
        Name of the column containing the query to be used for searching. This will only be required if the task is search. Each query will be run against the entire index, not individual videos.

    summarization_type : str, Optional
        Type of the summary to be generated. This will only be required if the task is summarize. Supported types are 'summary', 'chapter' and 'highlight'.

    prompt : str, Optional
        Prompt to be used for the summarize. This will only be required if the type is summary, chapter or highlight.
    For more information, refer the API reference: https://docs.twelvelabs.io/reference/api-reference
    """

    index_name: str
    engine_id: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    index_options: List[str]
    addons: List[str] = []
    video_urls: Optional[List[str]] = None
    video_urls_column: Optional[str] = None
    video_files: Optional[List[str]] = None
    video_files_column: Optional[str] = None
    task: str = None
    search_options: Optional[List[str]] = None
    search_query_column: Optional[str] = None
    summarization_type: Optional[str] = None
    prompt: Optional[str] = None

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

    @model_validator(mode="before")
    @classmethod
    def check_for_valid_task(cls, values: Any) -> Any:
        """
        Root validator to check if the task provided is valid.

        Parameters
        ----------
        values : Dict
            Dictionary containing the attributes of the model.

        Raises
        ------
        ValueError
            If the task provided is not valid.
        """

        task = values.get("task")

        if task and task not in ["search", "summarization"]:
            raise ValueError(
                f"task {task} is not supported. Please provide a valid task."
            )

        return values

    @model_validator(mode="before")
    @classmethod
    def check_for_valid_engine_options(cls, values: Any) -> Any:
        """
        Root validator to check if the options specified for particular engines are valid.

        Parameters
        ----------
        values : Dict
            Dictionary containing the attributes of the model.

        Raises
        ------
        ValueError
            If there are any typos in the parameters.
        """

        engine_id = values.get("engine_id")
        index_options = values.get("index_options")

        if engine_id and 'pegasus' in engine_id:
            if not set(index_options).issubset(set(['visual', 'conversation'])):
                raise ValueError(
                    "index_optios for the Pegasus family of video understanding engines should be one or both of the following engine options: visual and conversation.."
                )

        return values

    @model_validator(mode="before")
    @classmethod
    def check_for_video_urls_or_video_files(cls, values: Any) -> Any:
        """
        Root validator to check if video_urls or video_files have been provided.

        Parameters
        ----------
        values : Dict
            Dictionary containing the attributes of the model.

        Raises
        ------
        ValueError
            If neither video_urls, video_files, video_urls_column nor video_files_column have been provided.

        """

        video_urls = values.get("video_urls")
        video_urls_column = values.get("video_urls_column")
        video_files = values.get("video_files")
        video_files_column = values.get("video_files_column")

        if not video_urls and not video_files and not video_urls_column and not video_files_column:
            raise ValueError(
                "Neither video_urls, video_files, video_urls_column nor video_files_column have been provided. Please provide one of them."
            )

        return values

    @model_validator(mode="before")
    @classmethod
    def check_for_task_specific_parameters(cls, values: Any) -> Any:
        """
        Root validator to check if task has been provided along with the other relevant parameters for each task.

        Parameters
        ----------
        values : Dict
            Dictionary containing the attributes of the model.

        Raises
        ------
        ValueError
            If the relevant parameters for the task have not been provided.
        """

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

            search_query_column = values.get("search_query_column")
            if not search_query_column:
                raise ValueError(
                    "search_query_column has not been provided. Please provide query_column."
                )

        elif task == "summarization":
            summarization_type = values.get("summarization_type")
            if summarization_type:
                if summarization_type not in ["summary", "chapter", "highlight"]:
                    raise ValueError(
                        "summarization_type should be one of the following: 'summary', 'chapter' and 'highlight'."
                    )

            else:
                raise ValueError(
                    "type has not been provided. Please provide summarization_type."
                )

        else:
            raise ValueError(
                f"task {task} is not supported. Please provide a valid task."
            )

        return values


class TwelveLabsHandlerConfig(BaseSettings):
    """
    Configuration for Twelve Labs handler.

    Attributes
    ----------

    BASE_URL : str
        Base URL for the Twelve Labs API.

    DEFAULT_ENGINE : str
        Default engine for the Twelve Labs API.

    DEFAULT_WAIT_DURATION : int
        Default wait duration when polling video indexing tasks created via the Twelve Labs API.
    """
    BASE_URL: str = "https://api.twelvelabs.io/v1.2"
    DEFAULT_ENGINE: str = "marengo2.6"
    DEFAULT_WAIT_DURATION: int = 5


twelve_labs_handler_config = TwelveLabsHandlerConfig()
