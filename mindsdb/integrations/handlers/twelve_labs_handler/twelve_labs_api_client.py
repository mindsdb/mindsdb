import time
import requests
from typing import Dict, List
from requests_toolbelt.multipart.encoder import MultipartEncoder

from mindsdb.utilities import log


logger = log.getLogger(__name__)

# TODO: move to config
BASE_URL = "https://api.twelvelabs.io/v1.1"
DEFAULT_ENGINE = "marengo2.5"
DEFAULT_WAIT_DURATION = 5


class TwelveLabsAPIClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'x-api-key': self.api_key
        }

    def create_index(self, index_name: str, index_options: List[str], engine_id: str = None, addons: List[str] = None) -> str:
        """
        Create an index.

        """
        body = {
            "index_name": index_name,
            "engine_id": engine_id if engine_id else DEFAULT_ENGINE,
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

        """
        body = {
            "index_id": index_id,
        }

        if video_url:
            body['video_url'] = video_url
        elif video_file:
            body['video_file'] = video_file

        result = self._submit_multi_part_request(
            method="POST",
            endpoint="tasks",
            data=body,
        )

        task_id = result['_id']
        logger.info(f"Created video indexing task {task_id} for {video_url if video_url else video_file} successfully.")
        return task_id

    def poll_for_video_indexing_tasks(self, task_ids: List[str]) -> None:
        """
        Poll for video indexing tasks to complete.

        """
        for task_id in task_ids:
            logger.info(f"Polling status of video indexing task {task_id}.")
            is_task_running = True

            while is_task_running:
                task = self._get_video_indexing_task(task_id=task_id)
                status = task['status']
                logger.info(f"Task {task_id} is in the {status} state.")

                wait_durtion = task['process']['remain_seconds'] if 'process' in task else DEFAULT_WAIT_DURATION

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

        while('next_page_token' in result['page_info']):
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

        """
        url = f"{BASE_URL}/{endpoint}"

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

        """
        url = f"{BASE_URL}/{endpoint}"

        headers = headers = headers if headers else self.headers

        multipart_data = MultipartEncoder(fields=data)
        headers['Content-Type'] = multipart_data.content_type

        if method == "POST":
            response = requests.post(
                url=url,
                headers=headers,
                data=multipart_data if multipart_data else {},
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
