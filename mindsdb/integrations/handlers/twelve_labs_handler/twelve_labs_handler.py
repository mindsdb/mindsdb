import os
import time
import requests
from typing import Optional, Dict, List
from requests_toolbelt.multipart.encoder import MultipartEncoder

import pandas as pd
from mindsdb.utilities import log

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key


logger = log.getLogger(__name__)

# TODO: move to config
BASE_URL = "https://api.twelvelabs.io/v1.1"
DEFAULT_ENGINE = "marengo2.5"
DEFAULT_WAIT_DURATION = 5


class TwelveLabsHandler(BaseMLEngine):
    """
    Integration with the Twelve Labs API.
    """

    name = 'twelve_labs'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        pass

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        # check for USING clause
        if 'using' not in args:
            # TODO: update Exception to InsufficientParametersException
            raise Exception("Twelve Labs engine requires a USING clause! Refer to its documentation for more details.")

        # get USING args and add target
        args = args['using']
        args['target'] = target

        # get api key
        api_key = get_api_key(
            api_name=self.name,
            create_args=args,
            engine_storage=self.engine_storage,
        )

        # update args with api key
        args['api_key'] = api_key

        # store args in model_storage
        self.model_storage.json_set('args', args)

        # get index if it exists
        index_id = self._get_index_by_name(index_name=args['index_name'])

        # create index if it doesn't exist
        if not index_id:
            logger.info(f"Index {args['index_name']} does not exist. Creating index.")
            index_id = self._create_index(
                index_name=args['index_name'],
                engine_id=args['engine_id'] if 'engine_id' in args else None,
                index_options=args['index_options'],
                addons=args['addons'] if 'addons' in args else []
            )
        
        else:
            logger.info(f"Index {args['index_name']} already exists. Using existing index.")

        # create video indexing tasks for all video files or video urls
        # video urls will be given precedence
        # check if video_urls_col has been set and use it to get the video urls
        if 'video_urls_col' in args:
            logger.info("video_urls_col has been set, therefore, it will be given precedence.")
            video_urls = df[args['video_urls_col']].tolist()
        
        # else, check if video_files_col has been set and use it to get the video files
        elif 'video_files_col' in args:
            logger.info("video_urls_col has not been set, therefore, video_files_col will be used.")
            video_files = df[args['video_files_col']].tolist()

        # else, check if video_urls or video_files have been set and use them
        else:
            logger.info("video_urls_col and video_files_col have not been set, therefore, video_urls and video_files will be used.")
            video_urls = ['video_urls'] if 'video_urls' in args else None
            video_files = ['video_files'] if 'video_files' in args else None

        # if video_urls and video_files are not set, then raise an exception
        if not video_urls and not video_files:
            logger.error("Neither video_urls_col, video_files_col, video_urls nor video_files have been set.")
            raise RuntimeError("Neither video_urls_col, video_files_col, video_urls nor video_files have been set. Please set one of them.")

        task_ids = self._create_video_indexing_tasks(
            index_id=index_id,
            video_urls=video_urls,
            video_files=video_files,
        )

        # poll for video indexing tasks to complete
        self._poll_for_video_indexing_tasks(task_ids=task_ids)

    def _create_index(self, index_name: str, index_options: List[str], engine_id: str  = None, addons: List[str] = None) -> str:
        """
        Create an index.
        
        """
        body = {
            "index_name": index_name,
            "engine_id": engine_id if engine_id else DEFAULT_ENGINE,
            "index_options": index_options,
            "addons": addons,
        }

        response = self._submit_request(
            method="POST",
            endpoint="indexes",
            data=body,
        )

        if response.status_code == 201:
            logger.info(f"Index {index_name} successfully created.")
            return response.json()['_id']
        elif response.status_code == 400:
            logger.error(f"Index {index_name} could not be created.")
            # TODO: update Exception to be more specific
            raise Exception(f"Index {index_name} could not be created. API request has failed: {response.json()['message']}")

    def _get_index_by_name(self, index_name: str) -> str:
        """
        Get an index by name.

        """
        params = {
            "index_name": index_name,
        }

        response = self._submit_request(
            method="GET",
            endpoint="indexes",
            data=params,
        )

        if response.status_code == 200:
            result = response.json()['data']
            return result[0]['_id'] if result else None
        elif response.status_code == 400:
            logger.error(f"Index {index_name} could not be retrieved.")
            # TODO: update Exception to be more specific
            raise Exception(f"Index {index_name} could not be retrieved. API request has failed: {response.json()['message']}")

    def _create_video_indexing_tasks(self, index_id: str, video_urls: List[str] = None, video_files: List[str] = None) -> List[str]:
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

        response = self._submit_multi_part_request(
            method="POST",
            endpoint="tasks",
            data=body,
        )
        
        if response.status_code == 201:
            task_id = response.json()['_id']
            logger.info(f"Created video indexing task {task_id} for {video_url if video_url else video_file} successfully.")
            return task_id
        elif response.status_code == 400:
            logger.error(f"Video indexing task for {video_url if video_url else video_file} could not be created.")
            # TODO: update Exception to be more specific
            raise Exception(f"Video indexing task for {video_url if video_url else video_file} could not be created. API request has failed: {response.json()['message']}")

    def _poll_for_video_indexing_tasks(self, task_ids: List[str]) -> None:
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
        response = self._submit_request(
            method="GET",
            endpoint=f"tasks/{task_id}",
        )

        if response.status_code == 200:
            logger.info(f"Retrieved video indexing task {task_id} successfully.")
            return response.json()
        elif response.status_code == 400:
            logger.error(f"Video indexing task {task_id} could not be retrieved.")
            # TODO: update Exception to be more specific
            raise Exception(f"Video indexing task {task_id} could not be retrieved. API request has failed: {response.json()['message']}")

    def _submit_request(self, endpoint: str, headers: Dict = None, data: Dict = None, method: str = "GET") -> Dict:
        """
        Submit a request to the Twelve Labs API.

        """
        url = f"{BASE_URL}/{endpoint}"

        headers = headers if headers else self._get_headers(api_key=self.model_storage.json_get('args')['api_key'])

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

        return response

    def _submit_multi_part_request(self, endpoint: str, headers: Dict = None, data: Dict = None, method: str = "POST") -> Dict:
        """
        Submit a multi-part request to the Twelve Labs API.

        """
        url = f"{BASE_URL}/{endpoint}"

        headers = headers if headers else self._get_headers(api_key=self.model_storage.json_get('args')['api_key'])

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

        return response        

    def _get_headers(self, api_key: str) -> Dict:
        return {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass