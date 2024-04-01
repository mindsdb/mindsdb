from typing import Optional, Dict
import pandas as pd

from mindsdb.integrations.handlers.stabilityai_handler.stabilityai import StabilityAPIClient

from mindsdb.integrations.libs.base import BaseMLEngine

from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key


logger = log.getLogger(__name__)


class StabilityAIHandler(BaseMLEngine):
    name = "stabilityai"

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        args = args['using']

        available_tasks = ["text-to-image", "image-to-image", "image-upscaling", "image-masking"]

        if 'task' not in args:
            raise Exception('task has to be specified. Available tasks are - ' + available_tasks)

        if args['task'] not in available_tasks:
            raise Exception('Unknown task specified. Available tasks are - ' + available_tasks)

        if 'local_directory_path' not in args:
            raise Exception('local_directory_path has to be specified')

        client = StabilityAPIClient(args["stabilityai_api_key"], args["local_directory_path"])

        if "engine_id" in args:
            if not client._is_valid_engine(args["engine_id"]):
                raise Exception("Unknown engine. The available engines are - " + list(client.available_engines.keys()))
        else:
            args["engine_id"] = "stable-diffusion-xl-1024-v1-0"

        if "upscale_engine_id" in args:
            if not client._is_valid_engine(args["upscale_engine_id"]):
                raise Exception("Unknown engine. The available engines are - " + list(client.available_engines.keys()))
        else:
            args["upscale_engine_id"] = "esrgan-v1-x2plus"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Stability AI Inference engine requires a USING clause! Refer to its documentation for more details.")
        self.generative = True

        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

    def _get_stability_client(self, args):
        api_key = get_api_key('stabilityai', args["using"], self.engine_storage, strict=False)

        local_directory_path = args["local_directory_path"]
        engine_id = args.get('engine_id', "stable-diffusion-xl-1024-v1-0")

        return StabilityAPIClient(api_key=api_key, dir_to_save=local_directory_path, engine=engine_id)

    def _process_text_image(self, df, args):

        def generate_text_image(conds, client):
            conds = conds.to_dict()
            return client.text_to_image(prompt=conds.get("text"), height=conds.get("height", 1024), width=conds.get("width", 1024))

        supported_params = set(["text", "height", "width"])

        if "text" not in df.columns:
            raise Exception("`text` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for text to image - {supported_params}")

        client = self._get_stability_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_text_image, client=client, axis=1)

    def _process_image_image(self, df, args):

        def generate_image_image(conds, client):
            conds = conds.to_dict()
            return client.image_to_image(image_url=conds.get("image_url"), prompt=conds.get("text"), height=conds.get("height", 1024), width=conds.get("width", 1024))

        supported_params = set(["image_url", "text", "height", "width"])

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for image to image - {supported_params}")

        client = self._get_stability_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_image_image, client=client, axis=1)

    def _process_image_upscaling(self, df, args):

        def generate_image_upscaling(conds, client):
            conds = conds.to_dict()
            return client.image_upscaling(image_url=conds.get("image_url"), prompt=conds.get("text"), height=conds.get("height"), width=conds.get("width"))

        supported_params = set(["image_url", "text", "height", "width"])

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for image scaling - {supported_params}")

        client = self._get_stability_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_image_upscaling, client=client, axis=1)

    def _process_image_masking(self, df, args):

        def generate_image_mask(conds, client):
            conds = conds.to_dict()
            return client.image_to_image(image_url=conds.get("image_url"), prompt=conds.get("text"), height=conds.get("height"), width=conds.get("width"), mask_image_url=conds.get("mask_image_url"))

        supported_params = set(["image_url", "text", "height", "width", "mask_image_url"])

        if "image_url" not in df.columns:
            raise Exception("`image_url` column has to be given in the query.")

        if "mask_image_url" not in df.columns:
            raise Exception("`mask_image_url` column has to be given in the query.")

        for col in df.columns:
            if col not in supported_params:
                raise Exception(f"Unknown column {col}. Currently supported parameters for image masking - {supported_params}")

        client = self._get_stability_client(args)

        return df[df.columns.intersection(supported_params)].apply(generate_image_mask, client=client, axis=1)

    def predict(self, df, args=None):

        args = self.model_storage.json_get('args')

        if args["task"] == "text-to-image":
            preds = self._process_text_image(df, args)
        elif args["task"] == "image-to-image":
            preds = self._process_image_image(df, args)
        elif args["task"] == "image-upscaling":
            preds = self._process_image_upscaling(df, args)
        elif args["task"] == "image-masking":
            preds = self._process_image_masking(df, args)

        result_df = pd.DataFrame()

        result_df['predictions'] = preds

        result_df = result_df.rename(columns={'predictions': args['target']})

        return result_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        client = StabilityAPIClient(args["stabilityai_api_key"], "")
        engine_id = args["engine_id"]
        upscale_engine_id = args["upscale_engine_id"]
        engine_id_res = client.available_engines.get(engine_id)
        upscale_engine_id_res = client.available_engines.get(upscale_engine_id)
        return pd.json_normalize([engine_id_res, upscale_engine_id_res])
