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
        self.default_linguistic_feature = 'ner'
        self.linguistic_features = ['ner', 'lemmatization', 'dependency-parsing', 'pos-tag', 'morphology']

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("spaCy engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if len(set(args.keys()) & {'linguistic_feature', 'target_column'}) == 0:
            raise Exception('`linguistic_feature` and `target_column` are required for this engine.')

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model_args = args['using']
        model_args['target'] = target

        if not args.get('linguistic_feature'):
            args['linguistic_feature'] = self.default_linguistic_feature

        train_model = args.get('train_model', False)
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

        column_name = model_args['target_column']
        linguistic_feature = model_args.get('linguistic_feature')

        # Named Entity Recognition
        if linguistic_feature == 'ner':
            predictions = []
            for _, text in df.iterrows():
                doc = nlp(text[column_name])
                entities = {(ent.start_char, ent.end_char, ent.label_) for ent in doc.ents}
                predictions.append(entities)

            df[model_args['target']] = predictions
            return df

        # Lemmatization
        elif linguistic_feature == 'lemmatization':
            predictions = []
            for _, text in df.iterrows():
                doc = nlp(text[column_name])
                entities = {(token.lemma_) for token in doc}
                predictions.append(entities)

            df[model_args['target']] = predictions
            return df

        # Dependency Parsing
        elif linguistic_feature == 'dependency-parsing':
            predictions = []
            for _, text in df.iterrows():
                doc = nlp(text[column_name])
                entities = {(token.text, token.dep_, token.head.text, token.head.pos_, str([child for child in token.children])) for token in doc}
                predictions.append(entities)

            df[model_args['target']] = predictions
            return df

        # Part-of-speech tagging
        elif linguistic_feature == 'pos-tag':
            predictions = []
            for _, text in df.iterrows():
                doc = nlp(text[column_name])
                entities = {(token.text, token.lemma_, token.pos_, token.tag_, token.dep_, token.shape_, token.is_alpha, token.is_stop) for token in doc}
                predictions.append(entities)

            df[model_args['target']] = predictions
            return df

        # Morphology
        elif linguistic_feature == 'morphology':
            predictions = []
            for _, text in df.iterrows():
                doc = nlp(text[column_name])
                tokens = {(str(token), str(token.morph)) for token in doc}
                predictions.append(tokens)

            df[model_args['target']] = predictions
            return df

        else:
            df[model_args['target']] = []
            return df

