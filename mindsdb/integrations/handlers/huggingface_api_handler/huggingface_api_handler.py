import os
from typing import Dict, Optional

import pandas as pd
from hugging_py_face import NLP, AudioProcessing, ComputerVision

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config


class HuggingFaceInferenceAPIHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = "huggingface_api"

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:
        if "using" not in args:
            raise Exception(
                "Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details."
            )

        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:
        args = self.model_storage.json_get("args")
        api_key = self._get_huggingface_api_key(args)

        if args["using"]["task"] == "text-classification":
            nlp = NLP(api_key)
            result_df = nlp.text_classification_in_df(
                df,
                args["using"]["column"],
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "fill-mask":
            nlp = NLP(api_key)
            result_df = nlp.fill_mask_in_df(
                df,
                args["using"]["column"],
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "summarization":
            nlp = NLP(api_key)
            result_df = nlp.summarization_in_df(
                df,
                args["using"]["column"],
                args["using"]["parameters"] if "parameters" in args["using"] else None,
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "text-generation":
            nlp = NLP(api_key)
            result_df = nlp.text_generation_in_df(
                df,
                args["using"]["column"],
                args["using"]["parameters"] if "parameters" in args["using"] else None,
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "question-answering":
            nlp = NLP(api_key)
            result_df = nlp.question_answering_in_df(
                df,
                args["using"]["question_column"],
                args["using"]["context_column"],
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "sentence-similarity":
            nlp = NLP(api_key)
            result_df = nlp.sentence_similarity_in_df(
                df,
                args["using"]["source_sentence_column"],
                args["using"]["sentence_column"],
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "zero-shot-classification":
            nlp = NLP(api_key)
            result_df = nlp.zero_shot_classification_in_df(
                df,
                args["using"]["column"],
                args["using"]["candidate_labels"],
                args["using"]["parameters"] if "parameters" in args["using"] else None,
                args["using"]["options"] if "options" in args["using"] else None,
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "image-classification":
            cp = ComputerVision(api_key)
            result_df = cp.image_classification_in_df(
                df,
                args["using"]["column"],
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "object-detection":
            cp = ComputerVision(api_key)
            result_df = cp.object_detection_in_df(
                df,
                args["using"]["column"],
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "speech-recognition":
            ap = AudioProcessing(api_key)
            result_df = ap.speech_recognition_in_df(
                df,
                args["using"]["column"],
                args["using"]["model"] if "model" in args["using"] else None,
            )

        elif args["using"]["task"] == "audio-classification":
            ap = AudioProcessing(api_key)
            result_df = ap.audio_classification_in_df(
                df,
                args["using"]["column"],
                args["using"]["model"] if "model" in args["using"] else None,
            )

        else:
            raise Exception(f"Task {args['using']['task']} is not supported!")

        result_df = result_df.rename(columns={"predictions": args["target"]})
        return result_df

    def _get_huggingface_api_key(self, args, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. HUGGINGFACE_API_KEY env variable
            4. huggingface.api_key setting in config.json
        """  # noqa
        # 1
        if "api_key" in args:
            return args["api_key"]
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if "api_key" in connection_args:
            return connection_args["api_key"]
        # 3
        api_key = os.getenv("HUGGINGFACE_API_KEY")
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get("huggingface", {})
        if "api_key" in openai_cfg:
            return openai_cfg["api_key"]

        if strict:
            raise Exception(
                f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )  # noqa
