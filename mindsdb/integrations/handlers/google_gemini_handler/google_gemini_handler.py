import os
from typing import Dict, Optional

from PIL import Image
import requests
import numpy as np
from io import BytesIO
import json
import textwrap
import google.generativeai as genai
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
import concurrent.futures
import httpx

logger = log.getLogger(__name__)


class GoogleGeminiHandler(BaseMLEngine):
    """
    Integration with the Google generative AI Python Library
    """

    name = "google_gemini"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = "gemini-pro"
        self.default_embedding_model = "models/embedding-001"
        self.generative = True
        self.mode = "default"
        self._configure_proxy()

    def _configure_proxy(self):
        """Configure proxy settings for Google Generative AI"""
        proxy_url = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY') or os.getenv('ALL_PROXY')
        if proxy_url:
            logger.info(f"Configuring Google Gemini to use proxy: {proxy_url}")
            # Configure httpx client with proxy for genai
            import google.generativeai.client as genai_client

            # Create httpx client with proxy
            proxy_client = httpx.Client(proxies=proxy_url)

            # Monkey patch the genai client to use our proxy client
            original_request = genai_client._client.request if hasattr(genai_client, '_client') else None

            def proxy_request(*args, **kwargs):
                # Use our proxy client for requests
                return proxy_client.request(*args, **kwargs)

            # Try to set proxy in genai configuration
            try:
                genai.configure(transport='rest')
                logger.info("Successfully configured proxy for Google Gemini")
            except Exception as e:
                logger.warning(f"Could not configure proxy directly: {e}")
        else:
            logger.info("No proxy configuration found in environment variables")

    def _configure_genai_with_proxy(self, api_key):
        """Configure genai with proxy support"""
        proxy_url = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY') or os.getenv('ALL_PROXY')

        if proxy_url:
            logger.info(f"Configuring genai with proxy: {proxy_url}")
            # Set proxy environment variables for requests library
            os.environ['HTTP_PROXY'] = proxy_url
            os.environ['HTTPS_PROXY'] = proxy_url

            # Force REST transport to support proxy
            try:
                # Import the REST client
                from google.ai.generativelanguage_v1beta.services.generative_service import GenerativeServiceRestTransport

                # Configure genai to use REST transport (supports HTTP proxy)
                genai.configure(
                    api_key=api_key,
                    transport='rest',
                    client_options={'api_endpoint': 'https://generativelanguage.googleapis.com'}
                )
                logger.info("Successfully configured genai with REST transport and proxy")
            except Exception as e:
                logger.warning(f"REST transport configuration failed, trying default: {e}")
                try:
                    # Fallback to basic configuration with transport=rest
                    genai.configure(api_key=api_key, transport='rest')
                    logger.info("Successfully configured genai with basic REST transport")
                except Exception as e2:
                    logger.warning(f"All proxy configurations failed, using default: {e2}")
                    genai.configure(api_key=api_key)
        else:
            # Even without proxy, prefer REST transport for consistency
            try:
                genai.configure(api_key=api_key, transport='rest')
                logger.info("Configured genai with REST transport (no proxy)")
            except Exception as e:
                logger.warning(f"REST transport failed, using default: {e}")
                genai.configure(api_key=api_key)

    def _generate_content_with_proxy(self, prompts, api_key, model_name):
        """Generate content using direct REST API calls with proxy support"""
        import requests

        proxy_url = os.getenv('HTTP_PROXY') or os.getenv('HTTPS_PROXY') or os.getenv('ALL_PROXY')
        proxies = {
            'http': proxy_url,
            'https': proxy_url
        } if proxy_url else None

        results = []
        base_url = "https://generativelanguage.googleapis.com/v1beta/models"

        for prompt in prompts:
            try:
                url = f"{base_url}/{model_name}:generateContent"
                headers = {
                    'Content-Type': 'application/json',
                }
                data = {
                    "contents": [{
                        "parts": [{
                            "text": prompt
                        }]
                    }]
                }

                response = requests.post(
                    url,
                    headers=headers,
                    json=data,
                    params={'key': api_key},
                    proxies=proxies,
                    timeout=30
                )

                if response.status_code == 200:
                    result = response.json()
                    if 'candidates' in result and len(result['candidates']) > 0:
                        content = result['candidates'][0]['content']['parts'][0]['text']
                        results.append(content)
                        logger.info(f"Successfully generated content via proxy: {content[:50]}...")
                    else:
                        logger.warning("No content in response")
                        results.append("")
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    results.append("")

            except Exception as e:
                logger.error(f"Error generating content with proxy: {e}")
                results.append("")

        return results

    # Similiar to openai handler
    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "Gemini engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

        if (
            len(
                set(args.keys())
                & {
                    "img_url",
                    "input_text",
                    "question_column",
                    "prompt_template",
                    "json_struct",
                    "prompt",
                }
            )
            == 0
        ):
            raise Exception(
                "One of `question_column`, `prompt_template` or `json_struct` is required for this engine."
            )

        keys_collection = [
            ["prompt_template"],
            ["question_column", "context_column"],
            ["prompt", "user_column", "assistant_column"],
            ["json_struct", "input_text"],
            ["img_url", "ctx_column"],
        ]
        for keys in keys_collection:
            if keys[0] in args and any(
                x[0] in args for x in keys_collection if x != keys
            ):
                raise Exception(
                    textwrap.dedent(
                        """\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `json_struct`
                        4) a `prompt' and 'user_column' and 'assistant_column`
                        5) a `img_url` and optional `ctx_column` for mode=`vision`
                """
                    )
                )

        # for all args that are not expected, raise an error
        known_args = set()
        # flatten of keys_collection
        for keys in keys_collection:
            known_args = known_args.union(set(keys))

        # TODO: need a systematic way to maintain a list of known args
        known_args = known_args.union(
            {
                "target",
                "model_name",
                "mode",
                "title_column",
                "predict_params",
                "type",
                "max_tokens",
                "temperature",
                "api_key",
            }
        )

        unknown_args = set(args.keys()) - known_args
        if unknown_args:
            # return a list of unknown args as a string
            raise Exception(
                f"Unknown arguments: {', '.join(unknown_args)}.\n Known arguments are: {', '.join(known_args)}"
            )

    def create(self, target, args=None, **kwargs):
        args = args["using"]
        args["target"] = target
        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        pred_args = args["predict_params"] if args else {}
        args = self.model_storage.json_get("args")
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

        api_key = self._get_google_gemini_api_key(args)
        self._configure_genai_with_proxy(api_key)

        # Always use direct REST API calls with proxy support (works with or without proxy)
        logger.info("Using direct REST API calls for better proxy support")
        results = self._generate_content_with_proxy(prompts, api_key, args.get("model_name", self.default_model))

        pred_df = pd.DataFrame(results, columns=[args["target"]])
        return pred_df

    def _get_google_gemini_api_key(self, args, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. GOOGLE_GENAI_API_KEY env variable
            4. google_gemini.api_key setting in config.json
        """

        if "api_key" in args:
            return args["api_key"]
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if "api_key" in connection_args:
            return connection_args["api_key"]
        # 3
        api_key = os.getenv("GOOGLE_GENAI_API_KEY")
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        google_gemini_config = config.get("google_gemini", {})
        if "api_key" in google_gemini_config:
            return google_gemini_config["api_key"]

        if strict:
            raise Exception(
                'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )

    def embedding_worker(self, args: Dict, df: pd.DataFrame):
        if args.get("question_column"):
            prompts = list(df[args["question_column"]].apply(lambda x: str(x)))
            if args.get("title_column", None):
                titles = list(df[args["title_column"]].apply(lambda x: str(x)))
            else:
                titles = None

            api_key = self._get_google_gemini_api_key(args)
            self._configure_genai_with_proxy(api_key)
            model_name = args.get("model_name", self.default_embedding_model)
            task_type = args.get("type")
            task_type = f"retrieval_{task_type}"

            if task_type == "retrieval_query":
                results = [
                    str(
                        genai.embed_content(
                            model=model_name, content=query, task_type=task_type
                        )["embedding"]
                    )
                    for query in prompts
                ]
            elif titles:
                results = [
                    str(
                        genai.embed_content(
                            model=model_name,
                            content=doc,
                            task_type=task_type,
                            title=title,
                        )["embedding"]
                    )
                    for title, doc in zip(titles, prompts)
                ]
            else:
                results = [
                    str(
                        genai.embed_content(
                            model=model_name, content=doc, task_type=task_type
                        )["embedding"]
                    )
                    for doc in prompts
                ]

            pred_df = pd.DataFrame(results, columns=[args["target"]])
            return pred_df
        else:
            raise Exception("Embedding mode needs a question_column")

    def vision_worker(self, args: Dict, df: pd.DataFrame):
        def get_img(url):
            # URL Validation
            response = requests.get(url)
            if response.status_code == 200 and response.headers.get(
                "content-type", ""
            ).startswith("image/"):
                return Image.open(BytesIO(response.content))
            else:
                raise Exception(f"{url} is not vaild image URL..")

        if args.get("img_url"):
            urls = list(df[args["img_url"]].apply(lambda x: str(x)))

        else:
            raise Exception("Vision mode needs a img_url")

        prompts = None
        if args.get("ctx_column"):
            prompts = list(df[args["ctx_column"]].apply(lambda x: str(x)))

        api_key = self._get_google_gemini_api_key(args)
        self._configure_genai_with_proxy(api_key)
        model = genai.GenerativeModel("gemini-pro-vision")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Download images concurrently using ThreadPoolExecutor
            imgs = list(executor.map(get_img, urls))
        # imgs = [Image.open(BytesIO(requests.get(url).content)) for url in urls]
        if prompts:
            results = [
                model.generate_content([img, text]).text
                for img, text in zip(imgs, prompts)
            ]
        else:
            results = [model.generate_content(img).text for img in imgs]

        pred_df = pd.DataFrame(results, columns=[args["target"]])

        return pred_df

    # Disclaimer: The following code has been adapted from the OpenAI handler.
    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
        elif attribute == "metadata":
            api_key = self._get_google_gemini_api_key(args)
            self._configure_genai_with_proxy(api_key)
            model_name = args.get("model_name", self.default_model)

            meta = genai.get_model(f"models/{model_name}").__dict__
            return pd.DataFrame(meta.items(), columns=["key", "value"])
        else:
            tables = ["args", "metadata"]
            return pd.DataFrame(tables, columns=["tables"])
