import json
from typing import Optional, Dict
import pandas as pd
from huggingface_hub import HfApi
from huggingface_hub import hf_hub_download
from hugging_py_face import NLP, ComputerVision, AudioProcessing, get_in_df_supported_tasks

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key
from .exceptions import UnsupportedTaskException, InsufficientParametersException


class HuggingFaceInferenceAPIHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_api'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        args = args['using']

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

        if args['task'] not in get_in_df_supported_tasks():
            raise UnsupportedTaskException(f'The task {args["task"]} is not supported by the Hugging Face Inference API engine.')

        if args['task'] == 'zero-shot-classification':
            if 'candidate_labels' not in args:
                raise Exception('"candidate_labels" is required for zero-shot-classification')

        if args['task'] == 'sentence-similarity':
            if 'input_column2' not in args:
                raise InsufficientParametersException('input_column2 has to be specified')

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise InsufficientParametersException("Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details.")

        args = args['using']
        args['target'] = target

        if 'options' not in args:
            args['options'] = {}

        if 'parameters' not in args:
            args['parameters'] = {}

        if args['model_name'] is not None:
            # config.json
            config = {}
            try:
                config_path = hf_hub_download(args['model_name'], 'config.json')
                config = json.load(open(config_path))
            except Exception:
                pass

            if 'max_length' in args:
                args['options']['max_length'] = args['max_length']
            elif 'max_position_embeddings' in config:
                args['options']['max_length'] = config['max_position_embeddings']
            elif 'max_length' in config:
                args['options']['max_length'] = config['max_length']

            labels_default = config.get('id2label', {})
            labels_map = {}
            if 'labels' in args:
                for num, value in labels_default.items():
                    if num.isdigit():
                        num = int(num)
                        labels_map[value] = args['labels'][num]
            args['labels_map'] = labels_map
            if 'task_specific_params' in config:
                args['task_specific_params'] = config['task_specific_params']

        # for summarization
        if 'min_output_length' in args:
            args['options']['min_output_length'] = args['min_output_length']

        if 'max_output_length' in args:
            args['options']['max_output_length'] = args['max_output_length']

        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = self.model_storage.json_get('args')
        api_key = get_api_key('huggingface_api', args, self.engine_storage, strict=False)

        input_column = args['input_column']
        model_name = args['model_name']
        endpoint = args['endpoint'] if 'endpoint' in args else None
        options = args['options'] if 'options' in args else None
        parameters = args['parameters'] if 'parameters' in args else None

        if args['task'] == 'text-classification':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.text_classification_in_df(
                df,
                input_column,
                options,
                model_name,
            )
            labels_map = args.get('labels_map')

            result_df['predictions'] = result_df['predictions'].apply(lambda x: labels_map.get(x, x))

        elif args['task'] == 'fill-mask':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.fill_mask_in_df(
                df,
                input_column,
                options,
                model_name
            )

        elif args['task'] == 'summarization':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.summarization_in_df(
                df,
                input_column,
                parameters,
                options,
                model_name
            )

        elif args['task'] == 'text-generation':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.text_generation_in_df(
                df,
                input_column,
                parameters,
                options,
                model_name
            )

        elif args['task'] == 'question-answering':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.question_answering_in_df(
                df,
                input_column,
                args['context_column'],
                model_name
            )

        elif args['task'] == 'sentence-similarity':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.sentence_similarity_in_df(
                df,
                input_column,
                args['input_column2'],
                options,
                model_name
            )

        elif args['task'] == 'zero-shot-classification':
            nlp = NLP(api_key, endpoint)
            result_df = nlp.zero_shot_classification_in_df(
                df,
                input_column,
                args['candidate_labels'],
                parameters,
                options,
                model_name
            )

        elif args['task'] == 'translation':
            lang_in = args['lang_input']
            lang_out = args['lang_output']

            input_origin = None
            if 'task_specific_params' in args:
                task = f"translation_{lang_in}_to_{lang_out}"
                if task in args['task_specific_params'] and 'prefix' in args['task_specific_params'][task]:
                    # inject prefix to data
                    prefix = args['task_specific_params'][task]['prefix']
                    input_origin = df[input_column]
                    df[input_column] = prefix + input_origin
                    # don't pick up model in hugging_py_face
                    lang_in = lang_out = None

            nlp = NLP(api_key, endpoint)
            result_df = nlp.translation_in_df(
                df,
                input_column,
                lang_in,
                lang_out,
                options,
                model_name
            )
            if input_origin is not None:
                df[input_column] = input_origin

        elif args['task'] == 'image-classification':
            cp = ComputerVision(api_key, endpoint)
            result_df = cp.image_classification_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'object-detection':
            cp = ComputerVision(api_key, endpoint)
            result_df = cp.object_detection_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'automatic-speech-recognition':
            ap = AudioProcessing(api_key, endpoint)
            result_df = ap.automatic_speech_recognition_in_df(
                df,
                input_column,
                model_name
            )

        elif args['task'] == 'audio-classification':
            ap = AudioProcessing(api_key, endpoint)
            result_df = ap.audio_classification_in_df(
                df,
                input_column,
                model_name
            )

        result_df = result_df.rename(columns={'predictions': args['target']})
        return result_df
