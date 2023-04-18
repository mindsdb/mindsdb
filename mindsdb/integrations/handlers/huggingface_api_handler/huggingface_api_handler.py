from typing import Optional, Dict

import pandas as pd

from hugging_py_face import NLP, ComputerVision, AudioProcessing

from mindsdb.integrations.libs.base import BaseMLEngine


class HuggingFaceInferenceAPIHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_inference_api'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details.")

        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = self.model_storage.json_get('args')

        if args['using']['task'] == 'text-classification':
            nlp = NLP(args['using']['api_key'])
            result_df = nlp.text_classification_in_df(
                df,
                args['using']['column'],
                args['using']['options'] if 'options' in args['using'] else None,
                args['using']['model'] if 'model' in args['using'] else None
            )

        elif args['using']['task'] == 'fill-mask':
            nlp = NLP(args['using']['api_key'])
            result_df = nlp.fill_mask_in_df(
                df,
                args['using']['column'],
                args['using']['options'] if 'options' in args['using'] else None,
                args['using']['model'] if 'model' in args['using'] else None
            )

        elif args['using']['task'] == 'summarization':
            nlp = NLP(args['using']['api_key'])
            result_df = nlp.summarization_in_df(
                df,
                args['using']['column'],
                args['using']['options'] if 'options' in args['using'] else None,
                args['using']['model'] if 'model' in args['using'] else None
            )

        else:
            raise Exception(f"Task {args['using']['task']} is not supported!")

        result_df = result_df.rename(columns={'predictions': args['target']})
        return result_df