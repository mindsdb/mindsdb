import spacy
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Optional

class SpacyHandler(BaseMLEngine):
    """
    Integration with the spaCy NLP library.
    """

    name = 'spacy'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("spaCy engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        train_model = args.get('train_model', False)
        model_args = {}
        model_args['target'] = target

        if train_model:
            pass
        else:
            # If not training, use the default English model
            nlp = spacy.load("en_core_web_sm")

        # Serialize the model
        bytes_data = nlp.to_bytes()

        # Save the serialized model and its config and model_args to the model storage
        self.model_storage.file_set('model_data', bytes_data)
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.json_set('config', nlp.config)

    def predict(self, df, args=None):

        config = self.model_storage.json_get('config')
        model_args = self.model_storage.json_get("model_args")
        bytes_data = self.model_storage.file_get('model_data')

        # Deserialize the model using the provided config
        lang_cls = spacy.util.get_lang_class(config["nlp"]["lang"])
        nlp = lang_cls.from_config(config)
        nlp.from_bytes(bytes_data)

        column_name = df.columns[0]

        predictions = []
        for text in df[column_name]:
            doc = nlp(text)
            # For now, assuming only interested in NER predictions
            entities = {(ent.start_char, ent.end_char, ent.label_) for ent in doc.ents}
            predictions.append(entities)

        df[model_args['target']] = predictions
        return df
