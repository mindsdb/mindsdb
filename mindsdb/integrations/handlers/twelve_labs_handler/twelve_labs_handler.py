import os
import json
import requests
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key

# TODO: move to config
BASE_URL = "https://api.twelvelabs.io/v1.1"
DEFAULT_ENGINE = "marengo2.5"


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

        # get headers for API requests
        headers = self._get_headers(api_key=api_key)

        # get index if it exists
        index = self._get_index_by_name(index_name=args['index_name'])
        index_id = index['id'] if index else None

        # create index if it doesn't exist
        if not index_id:
            index_id = self._create_index(
                index_name=args['index_name'],
                engine_id=args['engine_id'] if 'engine_id' in args else None,
                index_options=args['index_options'],
                addons=args['addons'] if 'addons' in args else None,
            )

        # create video indexing tasks for all video files or video urls
        # video urls will be given precedence
        task_ids = self._create_video_indexing_tasks(
            index_id=index_id,
            video_urls=args['video_urls'],
            video_files=args['video_files'],
        )

        # poll for video indexing tasks to complete
        self._poll_for_video_indexing_tasks(task_ids=task_ids)

        # store args in model_storage
        self.model_storage.json_set('args', args)

    def _create_index(self, index_name: str, engine_id: str  = None, index_options: List[str], addons: List[str] = []) -> str:
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
            headers=headers,
            data=body,
        )

        return response['_id']

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
            headers=headers,
            data=params,
        )

        return response['data'][0]['_id'] if response['data'] else None

    def _create_video_indexing_tasks(self, index_id: str, video_urls: List[str] = None, video_files: List[str] = None) -> List[str]:
        """
        Create video indexing tasks.

        """
        task_ids = []

        if video_urls:
            for video_url in video_urls:
                task_ids.append(
                    self._create_video_indexing_task(
                        index_id=index_id, 
                        video_url=video_url
                    )
                )
            
        if video_files:
            for video_file in video_files:
                task_ids.append(
                    self._create_video_indexing_task(
                        index_id=index_id, 
                        video_file=video_file
                    )
                )

        return task_ids

    def _create_video_indexing_task(self, index_id: str, video_url: str = None, video_file:  = None) -> str:
        """
        Create a video indexing task.

        """
        body = {
            "index_id": index_id,
            "video_url": video_url,
            "video_file": video_file,
        }

        response = self._submit_request(
            method="POST",
            endpoint="tasks",
            headers=headers,
            data=body,
        )

        return response['_id']

    def _poll_for_video_indexing_tasks(self, task_ids: List[str]) -> None:
        """
        Poll for video indexing tasks to complete.

        """
        pass

    def _get_video_indexing_task(self, task_id: str) -> Dict:
        """
        Get a video indexing task.

        """

        response = self._submit_request(
            method="GET",
            endpoint=f"tasks/{task_id}",
            headers=headers,
        )

        return response

    def _submit_request(self, method: str = "GET", endpoint: str, headers: Dict, data: Dict = None) -> Dict:
        """
        Submit a request to the Twelve Labs API.

        """
        url = f"{BASE_URL}/{endpoint}"

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
                data=data if data else {},
            )

        else:
            raise Exception(f"Method {method} not supported yet.")

        return response.json()

    def _get_headers(self, api_key: str) -> Dict:
        return {
            "x-api-key": api_key,
            "Content-Type": "application/json
        }

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass