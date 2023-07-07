import os
from typing import Optional, Dict

import pandas as pd

from huggingface_hub import HfApi
from hugging_py_face import NLP, ComputerVision, AudioProcessing

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine

from .exceptions import UnsupportedTaskException, InsufficientParametersException


class HuggingFaceInferenceAPIHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_api'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'input_column' not in args:
            raise InsufficientParametersException('input_column has to be specified')

        if 'model_name' not in args:
            # detect model by task
            task = args.get('task')
            if task is None:
                raise InsufficientParametersException('model_name or task have to be specified')

            args['model_name'] = None
        else:
            # detect task by model
            hf_api = HfApi()
            metadata = hf_api.model_info(args['model_name'])

            if 'task' not in args:
                args['task'] = metadata.pipeline_tag

        # TODO raise if task is not supported

        #TODO check columns for specific tasks

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details.")

        args = args['using']
        args['target'] = target

        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = self.model_storage.json_get('args')
        api_key = self._get_huggingface_api_key(args)

        input_column = args['input_column']
        model_name = args['model_name']

        if args['task'] == 'text-classification':
            nlp = NLP(api_key)
            result_df = nlp.text_classification_in_df(
                df,
                input_column,
                args['options'] if 'options' in args else None,
                model_name,
            )

        elif args['task'] == 'fill-mask':
            nlp = NLP(api_key)
            result_df = nlp.fill_mask_in_df(
                df,
                input_column,
                args['options'] if 'options' in args else None,
                model_name
            )

        elif args['task'] == 'summarization':
            nlp = NLP(api_key)
            result_df = nlp.summarization_in_df(
                df,
                input_column,
                args['parameters'] if 'parameters' in args else None,
                args['options'] if 'options' in args else None,
                model_name
            )

        elif args['task'] == 'text-generation':
            nlp = NLP(api_key)
            result_df = nlp.text_generation_in_df(
                df,
                input_column,
                args['parameters'] if 'parameters' in args else None,
                args['options'] if 'options' in args else None,
                model_name
            )

        elif args['task'] == 'question-answering':
            nlp = NLP(api_key)
            result_df = nlp.question_answering_in_df(
                df,
                input_column,
                args['context_column'],
                model_name
            )

        elif args['task'] == 'sentence-similarity':
            nlp = NLP(api_key)
            result_df = nlp.sentence_similarity_in_df(
                df,
                input_column,
                args['sentence_column'],  # TODO name it input_column2?
                args['options'] if 'options' in args else None,
                model_name
            )

        elif args['task'] == 'zero-shot-classification':
            nlp = NLP(api_key)
            result_df = nlp.zero_shot_classification_in_df(
                df,
                input_column,
                args['candidate_labels'],
                args['parameters'] if 'parameters' in args else None,
                args['options'] if 'options' in args else None,
                model_name
            )

        elif args['task'] == 'image-classification':
            cp = ComputerVision(api_key)
            result_df = cp.image_classification_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'object-detection':
            cp = ComputerVision(api_key)
            result_df = cp.object_detection_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'automatic-speech-recognition':
            ap = AudioProcessing(api_key)
            result_df = ap.automatic_speech_recognition_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'audio-classification':
            ap = AudioProcessing(api_key)
            result_df = ap.audio_classification_in_df(
                df,
                input_column,
                model_name
            )

        else:
            raise Exception(f"Task {args['task']} is not supported!")

        result_df = result_df.rename(columns={'predictions': args['target']})
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
        if 'api_key' in args:
            return args['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('HUGGINGFACE_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get('huggingface', {})
        if 'api_key' in openai_cfg:
            return openai_cfg['api_key']

        if strict:
            raise Exception(f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')  # noqa