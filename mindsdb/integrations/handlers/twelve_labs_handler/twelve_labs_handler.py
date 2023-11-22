import json
import os
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key


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

        # if index_name is not provided, create an index

        # create video indexing tasks for all video files or video urls
        # video urls will be given precedence

        # poll for video indexing tasks to complete

        # store args in model_storage
        self.model_storage.json_set('args', args)

    def _create_index(self, index_name: str, engine_id: str, index_options: List[str]) -> str:
        pass

    def _create_video_indexing_task(self, index_id: str, video_url: str, video_file: str) -> str:
        pass

    def _get_video_indexing_task(self, task_id: str) -> Dict:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass