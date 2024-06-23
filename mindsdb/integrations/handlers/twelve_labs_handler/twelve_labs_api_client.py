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

    def __init__(self, api_key: str, base_url: str = None):
        """
        The initializer for the TwelveLabsAPIClient.

        Parameters
        ----------
        api_key : str
            The Twelve Labs API key.
        base_url : str, Optional
            The base URL for the Twelve Labs API. Defaults to the base URL in the Twelve Labs handler settings.
        """

        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key
        }
        self.base_url = base_url if base_url else twelve_labs_handler_config.BASE_URL

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

        # TODO: change index_options to engine_options?
        # TODO: support multiple engines per index?
        body = {
            "index_name": index_name,
            "engines": [{
                "engine_name": engine_id if engine_id else twelve_labs_handler_config.DEFAULT_ENGINE,
                "engine_options": index_options
            }],
            "addons": addons,
        }

        result = self._submit_request(
            method="POST",
            endpoint="/indexes",
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
            endpoint="/indexes",
            data=params,
        )

        data = result['data']
        return data[0]['_id'] if data else None

    def list_videos_in_index(self, index_name: str) -> List[Dict]:
        """
        List videos in an index.

        Parameters
        ----------
        index_name : str
            Name of the index.

        Returns
        -------
        List[Dict]
            List of videos in the index.
        """

        index_id = self.get_index_by_name(index_name=index_name)

        data = []
        result = self._submit_request(
            method="GET",
            endpoint=f"/indexes/{index_id}/videos",
        )
        data.extend(result['data'])

        while result['page_info']['page'] < result['page_info']['total_page']:
            result = self._submit_request(
                method="GET",
                endpoint=f"/indexes/{index_id}/videos?page_token={result['page_info']['next_page_token']}",
            )
            data.extend(result['data'])

        logger.info(f"Retrieved videos in index {index_id} successfully.")

        return data

    def _update_video_metadata(self, index_id: str, video_id: str, video_title: str = None, metadata: Dict = None) -> None:
        """
        Update the metadata of a video that has already been indexed.

        Parameters
        ----------
        video_id : str
            ID of the video.

        video_title : str
            Title of the video.

        metadata : Dict, Optional
            Metadata to be updated.

        Returns
        -------
        None
        """

        body = {}

        if video_title:
            body['video_title'] = video_title

        if metadata:
            body['metadata'] = metadata

        self._submit_request(
            method="PUT",
            endpoint=f"/indexes/{index_id}/videos/{video_id}",
            data=body,
        )

        logger.info(f"Updated metadata for video {video_id} successfully.")

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
            endpoint="/tasks",
            data=body,
        )

        if file_to_close:
            file_to_close.close()

        task_id = result['_id']
        logger.info(f"Created video indexing task {task_id} for {video_url if video_url else video_file} successfully.")

        # update the video title
        video_reference = video_url if video_url else video_file
        task = self._get_video_indexing_task(task_id=task_id)
        self._update_video_metadata(
            index_id=index_id,
            video_id=task['video_id'],
            metadata={
                "video_reference": video_reference
            }
        )

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

        logger.info("All videos indexed successffully.")

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
            endpoint=f"/tasks/{task_id}",
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
            endpoint="/search",
            data=body,
        )
        data.extend(result['data'])

        while 'next_page_token' in result['page_info']:
            result = self._submit_request(
                method="GET",
                endpoint=f"/search/{result['page_info']['next_page_token']}"
            )
            data.extend(result['data'])

        logger.info(f"Search for index {index_id} completed successfully.")
        return data

    def summarize_videos(self, video_ids: List[str], summarization_type: str, prompt: str) -> Dict:
        """
        Summarize videos.

        Parameters
        ----------
        video_ids : List[str]
            List of video IDs.

        summarization_type : str
            Type of the summary to be generated. Supported types are 'summary', 'chapter' and 'highlight'.

        prompt: str
            Prompt to be used for the Summarize task

        Returns
        -------
        Dict
            Summary of the videos.
        """

        results = []
        results = [self.summarize_video(video_id, summarization_type, prompt) for video_id in video_ids]

        logger.info(f"Summarized videos {video_ids} successfully.")
        return results

    def summarize_video(self, video_id: str, summarization_type: str, prompt: str) -> Dict:
        """
        Summarize a video.

        Parameters
        ----------
        video_id : str
            ID of the video.

        summarization_type : str
            Type of the summary to be generated. Supported types are 'summary', 'chapter' and 'highlight'.

        prompt: str
            Prompt to be used for the Summarize task

        Returns
        -------
        Dict
            Summary of the video.
        """
        body = {
            "video_id": video_id,
            "type": summarization_type,
            "prompt": prompt
        }

        result = self._submit_request(
            method="POST",
            endpoint="/summarize",
            data=body,
        )

        logger.info(f"Video {video_id} summarized successfully.")
        return result

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

        headers = headers if headers else self.headers

        if method == "GET":
            response = requests.get(
                url=self.base_url + endpoint,
                headers=headers,
                params=data if data else {},
            )

        elif method == "POST":
            response = requests.post(
                url=self.base_url + endpoint,
                headers=headers,
                json=data if data else {},
            )

        elif method == "PUT":
            response = requests.put(
                url=self.base_url + endpoint,
                headers=headers,
                json=data if data else {},
            )

        else:
            raise Exception(f"Method {method} not supported yet.")

        return self._handle_response(response)

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

        headers = headers = headers if headers else self.headers

        multipart_data = MultipartEncoder(fields=data)
        headers['Content-Type'] = multipart_data.content_type
        if method == "POST":
            response = requests.post(
                url=self.base_url + endpoint,
                headers=headers,
                data=multipart_data if multipart_data else {}
            )

        else:
            raise Exception(f"Method {method} not supported yet.")
        return self._handle_response(response)

    def _handle_response(self, response):
        if response.status_code in (200, 201):
            if response.content:
                result = response.json()
                logger.info("API request was successful.")
                return result
            else:
                logger.info("API request was successful. No content returned.")
                return {}
        else:
            if response.content:
                logger.error(f"API request has failed: {response.content}")
                # TODO: update Exception to be more specific
                raise Exception(f"API request has failed: {response.content}")
            else:
                logger.error("API request has failed. No content returned.")
                raise Exception("API request has failed. No content returned.")
