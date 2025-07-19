from typing import Dict, Optional
import json
import textwrap
from mindsdb.utilities import log
from google.cloud.aiplatform import init, TabularDataset, Model, Endpoint
from vertexai.preview.generative_models import GenerativeModel, Part
from vertexai.language_models import TextEmbeddingModel

import pandas as pd
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
import concurrent.futures
import numpy as np


from PIL import Image
import requests
from io import BytesIO



from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleServiceAccountOAuth2Manager

logger = log.getLogger(__name__)


class VertexClient:
    """A class to interact with Vertex AI"""

    def __init__(self, args_json, credentials_url=None, credentials_file=None, credentials_json=None):
        self.default_model = "gemini-2.5-flash"
        self.default_embedding_model = "textembedding-gecko@003"
        self.generative = True
        self.mode = "default"
        google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
            credentials_url=credentials_url,
            credentials_file=credentials_file,
            credentials_json=credentials_json,
        )
        credentials = google_sa_oauth2_manager.get_oauth2_credentials()

        init(
            credentials=credentials,
            project=args_json["project_id"],
            location=args_json["location"],
            staging_bucket=args_json["staging_bucket"],
            # the name of the experiment to use to track
            # logged metrics and parameters
            experiment=args_json["experiment"],
            # description of the experiment above
            experiment_description=args_json["experiment_description"],
        )

    def print_datasets(self):
        """Print all datasets and dataset ids in the project"""
        for dataset in TabularDataset.list():
            logger.info(f"Dataset display name: {dataset.display_name}, ID: {dataset.name}")

    def print_models(self):
        """Print all model names and model ids in the project"""
        for model in Model.list():
            logger.info(f"Model display name: {model.display_name}, ID: {model.name}")

    def print_endpoints(self):
        """Print all endpoints and endpoint ids in the project"""
        for endpoint in Endpoint.list():
            logger.info(f"Endpoint display name: {endpoint.display_name}, ID: {endpoint.name}")

    def get_model_by_display_name(self, display_name):
        """Get a model by its display name"""
        try:
            return Model.list(filter=f'display_name="{display_name}"')[0]
        except IndexError:
            logger.info(f"Model with display name {display_name} not found")

    def get_endpoint_by_display_name(self, display_name):
        """Get an endpoint by its display name"""
        try:
            return Endpoint.list(filter=f'display_name="{display_name}"')[0]
        except IndexError:
            logger.info(f"Endpoint with display name {display_name} not found")

    def get_model_by_id(self, model_id):
        """Get a model by its ID"""
        try:
            return Model(model_name=model_id)
        except IndexError:
            logger.info(f"Model with ID {model_id} not found")

    def deploy_model(self, model):
        """Deploy a model to an endpoint - long runtime"""
        endpoint = model.deploy()
        return endpoint

    def predict_from_df(self, endpoint_display_name, df, custom_model=False):
        """Make a prediction from a Pandas dataframe"""
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        if custom_model:
            records = df.values.tolist()
        else:
            records = df.astype(str).to_dict(orient="records")  # list of dictionaries
        prediction = endpoint.predict(instances=records)
        return prediction

    def predict_from_csv(self, endpoint_display_name, csv_to_predict):
        """Make a prediction from a CSV file"""
        df = pd.read_csv(csv_to_predict)
        return self.predict_from_df(endpoint_display_name, df)

    def predict_from_dict(self, endpoint_display_name, data):

        # convert to list of dictionaries
        instances = [dict(zip(data.keys(), values)) for values in zip(*data.values())]
        endpoint = self.get_endpoint_by_display_name(endpoint_display_name)
        prediction = endpoint.predict(instances=instances)
        return prediction
    

    def gemini_predict_from_df(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        pred_args = args["predict_params"] if args else {}
        # args = self.model_storage.json_get("args")
       
        df = df.reset_index(drop=True)

        # same as opeani handler for getting prompt template and mode
        if pred_args.get("prompt_template", False):
            base_template = pred_args[
                "prompt_template"
            ]  # override with predict-time template if available
        elif args.get("prompt_template", False):
            base_template = args["prompt_template"]
        else:
            base_template = None

        # Embedding Mode
        if args.get("mode") == "embedding":
            args["type"] = pred_args.get("type", "query")
            return self.embedding_worker(args, df)

        elif args.get("mode") == "vision":
            return self.vision_worker(args, df)

        elif args.get("mode") == "conversational":
            # Enable chat mode using
            # https://ai.google.dev/tutorials/python_quickstart#chat_conversations
            # OR
            # https://github.com/google/generative-ai-python?tab=readme-ov-file#developers-who-use-the-palm-api
            pass

        else:
            if args.get("prompt_template", False):
                prompts, empty_prompt_ids = get_completed_prompts(base_template, df)

            # Disclaimer: The following code has been adapted from the OpenAI handler.
            elif args.get("context_column", False):
                empty_prompt_ids = np.where(
                    df[[args["context_column"], args["question_column"]]]
                    .isna()
                    .all(axis=1)
                    .values
                )[0]
                contexts = list(df[args["context_column"]].apply(lambda x: str(x)))
                questions = list(df[args["question_column"]].apply(lambda x: str(x)))
                prompts = [
                    f"Give only answer for: \nContext: {c}\nQuestion: {q}\nAnswer: "
                    for c, q in zip(contexts, questions)
                ]

                # Disclaimer: The following code has been adapted from the OpenAI handler.
            elif args.get("json_struct", False):
                empty_prompt_ids = np.where(
                    df[[args["input_text"]]].isna().all(axis=1).values
                )[0]
                prompts = []
                for i in df.index:
                    if "json_struct" in df.columns:
                        if isinstance(df["json_struct"][i], str):
                            df["json_struct"][i] = json.loads(df["json_struct"][i])
                        json_struct = ""
                        for ind, val in enumerate(df["json_struct"][i].values()):
                            json_struct = json_struct + f"{ind}. {val}\n"
                    else:
                        json_struct = ""
                        for ind, val in enumerate(args["json_struct"].values()):
                            json_struct = json_struct + f"{ind + 1}. {val}\n"

                    p = textwrap.dedent(
                        f"""\
                        Using text starting after 'The text is:', give exactly {len(args['json_struct'])} answers to the questions:
                        {{{{json_struct}}}}

                        Answers should be in the same order as the questions.
                        Answer should be in form of one JSON Object eg. {"{'key':'value',..}"} where key=question and value=answer.
                        If there is no answer to the question in the text, put a -.
                        Answers should be as short as possible, ideally 1-2 words (unless otherwise specified).

                        The text is:
                        {{{{{args['input_text']}}}}}
                    """
                    )
                    p = p.replace("{{json_struct}}", json_struct)
                    for column in df.columns:
                        if column == "json_struct":
                            continue
                        p = p.replace(f"{{{{{column}}}}}", str(df[column][i]))
                    prompts.append(p)
            elif "prompt" in args:
                empty_prompt_ids = []
                prompts = list(df[args["user_column"]])
            else:
                empty_prompt_ids = np.where(
                    df[[args["question_column"]]].isna().all(axis=1).values
                )[0]
                prompts = list(df[args["question_column"]].apply(lambda x: str(x)))

        # remove prompts without signal from completion queue
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

  
        

        # called gemini model withinputs
        model = GenerativeModel(args.get("model_name", self.default_model))
       
        results = []
        for m in prompts:
            results.append(model.generate_content(m).text)

        pred_df = pd.DataFrame(results, columns=[args["target"]])
        return pred_df

    
    def embedding_worker(self, args: Dict, df: pd.DataFrame):
        if args.get("question_column"):
            prompts = df[args["question_column"]].astype(str).tolist()
            model_name = args.get("model_name", self.default_embedding_model)
            if not model_name:
                model_name = "textembedding-gecko@003"  # default embedding model
            model = TextEmbeddingModel.from_pretrained(model_name)

            # Gecko does not use task_type/title – only pure embedding
            results = [str(model.get_embeddings([text])[0].values) for text in prompts]

            return pd.DataFrame(results, columns=[args["target"]])
        else:
            raise Exception("Embedding mode needs a question_column")
        
    def vision_worker(self, args: Dict, df: pd.DataFrame):
        def get_img_bytes(url):
            response = requests.get(url)
            if response.status_code == 200 and response.headers.get("content-type", "").startswith("image/"):
                return Part.from_data(data=response.content, mime_type="image/jpeg")
            else:
                raise Exception(f"{url} is not a valid image URL")

        if not args.get("img_url"):
            raise Exception("Vision mode needs an img_url")

        urls = df[args["img_url"]].astype(str).tolist()
        prompts = df[args["ctx_column"]].astype(str).tolist() if args.get("ctx_column") else None

        
        model = GenerativeModel("gemini-1.5-pro")

        with concurrent.futures.ThreadPoolExecutor() as executor:
            images = list(executor.map(get_img_bytes, urls))

        if prompts:
            results = [model.generate_content([img, text]).text for img, text in zip(images, prompts)]
        else:
            results = [model.generate_content(img).text for img in images]

        return pd.DataFrame(results, columns=[args["target"]])
