import time
import requests
from typing import Dict, List, Optional
from requests_toolbelt.multipart.encoder import MultipartEncoder

from mindsdb.utilities import log
from mindsdb.integrations.handlers.twelve_labs_handler.settings import twelve_labs_handler_config


logger = log.getLogger(__name__)


class TwelveLabsAPIClient:
    """
    The Twelve Labs API client for the Twelve Labs handler.
    This client is used for accessing the Twelve Labs API endpoints.
    """

    def __init__(self, api_key: str):
        """
        The initializer for the TwelveLabsAPIClient.

        Parameters
        ----------
        api_key : str
            The Twelve Labs API key.
        """

        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key
        }

    def create_index(self, index_name: str, index_options: List[str], engine_id: Optional[str] = None, addons: Optional[List[str]] = None) -> str:
        """
        Create an index.

        Parameters
        ----------
        index_name : str
            Name of the index to be created.

        index_options : List[str]
            List of that specifies how the platform will process the videos uploaded to this index.

        engine_id : str, Optional
            ID of the engine. If not provided, the default engine is used.

        addons : List[str], Optional
            List of addons that should be enabled for the index.

        Returns
        -------
        str
            ID of the created index.
        """

        body = {
            "index_name": index_name,
            "engine_id": engine_id if engine_id else twelve_labs_handler_config.DEFAULT_ENGINE,
            "index_options": index_options,
            "addons": addons,
        }

        result = self._submit_request(
            method="POST",
            endpoint="indexes",
            data=body,
        )

        logger.info(f"Index {index_name} successfully created.")
        return result['_id']

    def get_index_by_name(self, index_name: str) -> str:
        """
        Get an index by name.

        Parameters
        ----------
        index_name : str
            Name of the index to be retrieved.

        Returns
        -------
        str
            ID of the index.
        """

        params = {
            "index_name": index_name,
        }

        result = self._submit_request(
            method="GET",
            endpoint="indexes",
            data=params,
        )

        data = result['data']
        return data[0]['_id'] if data else None

    def create_video_indexing_tasks(self, index_id: str, video_urls: List[str] = None, video_files: List[str] = None) -> List[str]:
        """
        Create video indexing tasks.

        Parameters
        ----------
        index_id : str
            ID of the index.

        video_urls : List[str], Optional
            List of video urls to be indexed. Either video_urls or video_files should be provided. This validation is handled by TwelveLabsHandlerModel.

        video_files : List[str], Optional
            List of video files to be indexed. Either video_urls or video_files should be provided. This validation is handled by TwelveLabsHandlerModel.

        Returns
        -------
        List[str]
            List of task IDs created.
        """

        task_ids = []

        if video_urls:
            logger.info("video_urls has been set, therefore, it will be given precedence.")
            logger.info("Creating video indexing tasks for video urls.")

            for video_url in video_urls:
                task_ids.append(
                    self._create_video_indexing_task(
                        index_id=index_id,
                        video_url=video_url
                    )
                )

        elif video_files:
            logger.info("video_urls has not been set, therefore, video_files will be used.")
            logger.info("Creating video indexing tasks for video files.")
            for video_file in video_files:
                task_ids.append(
                    self._create_video_indexing_task(
                        index_id=index_id,
                        video_file=video_file
                    )
                )

        return task_ids

    def _create_video_indexing_task(self, index_id: str, video_url: str = None, video_file: str = None) -> str:
        """
        Create a video indexing task.

        Parameters
        ----------
        index_id : str
            ID of the index.

        video_url : str, Optional
            URL of the video to be indexed. Either video_url or video_file should be provided. This validation is handled by TwelveLabsHandlerModel.

        video_file : str, Optional
            Path to the video file to be indexed. Either video_url or video_file should be provided. This validation is handled by TwelveLabsHandlerModel.

        Returns
        -------
        str
            ID of the created task.
        """

        body = {
            "index_id": index_id,
        }
        file_to_close = None
        if video_url:
            body['video_url'] = video_url
        elif video_file:
            import mimetypes
            # WE need the file open for the duration of the request. Maybe simplify it with context manager later, but needs _create_video_indexing_task re-written
            file_to_close = open(video_file, 'rb')
            mime_type, _ = mimetypes.guess_type(video_file)
            body['video_file'] = (file_to_close.name, file_to_close, mime_type)

        result = self._submit_multi_part_request(
            method="POST",
            endpoint="tasks",
            data=body,
        )

        if file_to_close:
            file_to_close.close()

        task_id = result['_id']
        logger.info(f"Created video indexing task {task_id} for {video_url if video_url else video_file} successfully.")
        return task_id

    def poll_for_video_indexing_tasks(self, task_ids: List[str]) -> None:
        """
        Poll for video indexing tasks to complete.

        Parameters
        ----------
        task_ids : List[str]
            List of task IDs to be polled.

        Returns
        -------
        None
        """

        for task_id in task_ids:
            logger.info(f"Polling status of video indexing task {task_id}.")
            is_task_running = True

            while is_task_running:
                task = self._get_video_indexing_task(task_id=task_id)
                status = task['status']
                logger.info(f"Task {task_id} is in the {status} state.")

                wait_durtion = task['process']['remain_seconds'] if 'process' in task else twelve_labs_handler_config.DEFAULT_WAIT_DURATION

                if status in ('pending', 'indexing', 'validating'):
                    logger.info(f"Task {task_id} will be polled again in {wait_durtion} seconds.")
                    time.sleep(wait_durtion)

                elif status == 'ready':
                    logger.info(f"Task {task_id} completed successfully.")
                    is_task_running = False

                else:
                    logger.error(f"Task {task_id} failed with status {task['status']}.")
                    # TODO: update Exception to be more specific
                    raise Exception(f"Task {task_id} failed with status {task['status']}.")

        logger.info("All videos indexed successfully.")

    def _get_video_indexing_task(self, task_id: str) -> Dict:
        """
        Get a video indexing task.

        Parameters
        ----------
        task_id : str
            ID of the task.

        Returns
        -------
        Dict
            Video indexing task.
        """

        result = self._submit_request(
            method="GET",
            endpoint=f"tasks/{task_id}",
        )

        logger.info(f"Retrieved video indexing task {task_id} successfully.")
        return result

    def search_index(self, index_id: str, query: str, search_options: List[str]) -> Dict:
        """
        Search an index.

        Parameters
        ----------
        index_id : str
            ID of the index.

        query : str
            Query to be searched.

        search_options : List[str]
            List of search options to be used.

        Returns
        -------
        Dict
            Search results.
        """

        body = {
            "index_id": index_id,
            "query": query,
            "search_options": search_options
        }

        data = []
        result = self._submit_request(
            method="POST",
            endpoint="search",
            data=body,
        )
        data.extend(result['data'])

        while 'next_page_token' in result['page_info']:
            result = self._submit_request(
                method="GET",
                endpoint=f"search/{result['page_info']['next_page_token']}"
            )
            data.extend(result['data'])

        logger.info(f"Search for index {index_id} completed successfully.")
        return data

    def _submit_request(self, endpoint: str, headers: Dict = None, data: Dict = None, method: str = "GET") -> Dict:
        """
        Submit a request to the Twelve Labs API.

        Parameters
        ----------
        endpoint : str
            API endpoint.

        headers : Dict, Optional
            Headers to be used in the request.

        data : Dict, Optional
            Data to be used in the request.

        method : str, Optional
            HTTP method to be used in the request. Defaults to GET.

        Returns
        -------
        Dict
            Response from the API.
        """

        url = f"{twelve_labs_handler_config.BASE_URL}/{endpoint}"

        headers = headers if headers else self.headers

        if method == "GET":
            response = requests.get(
                url=url,
                headers=headers,
                params=data if data else {},
            )

        elif method == "POST":
            response = requests.post(
                url=url,
                headers=headers,
                json=data if data else {},
            )

        else:
            raise Exception(f"Method {method} not supported yet.")

        result = response.json()
        if response.status_code in (200, 201):
            logger.info("API request was successful.")
            return result
        else:
            logger.error(f"API request has failed: {result['message']}")
            # TODO: update Exception to be more specific
            raise Exception(f"API request has failed: {result['message']}")

    def _submit_multi_part_request(self, endpoint: str, headers: Dict = None, data: Dict = None, method: str = "POST") -> Dict:
        """
        Submit a multi-part request to the Twelve Labs API.

        Parameters
        ----------
        endpoint : str
            API endpoint.

        headers : Dict, Optional
            Headers to be used in the request.

        data : Dict, Optional
            Data to be used in the request.

        method : str, Optional
            HTTP method to be used in the request. Defaults to GET.

        Returns
        -------
        Dict
            Response from the API.
        """

        url = f"{twelve_labs_handler_config.BASE_URL}/{endpoint}"

        headers = headers = headers if headers else self.headers

        multipart_data = MultipartEncoder(fields=data)
        headers['Content-Type'] = multipart_data.content_type

        if method == "POST":
            response = requests.post(
                url=url,
                headers=headers,
                data=multipart_data if multipart_data else {}
            )

        else:
            raise Exception(f"Method {method} not supported yet.")

        result = response.json()
        if response.status_code in (200, 201):
            logger.info("API request was successful.")
            return result
        else:
            logger.error(f"API request has failed: {result['message']}")
            # TODO: update Exception to be more specific
            raise Exception(f"API request has failed: {result['message']}")
