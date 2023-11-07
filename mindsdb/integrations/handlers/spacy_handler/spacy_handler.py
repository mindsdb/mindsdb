import spacy
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Optional


def named_entity_recognition(doc, output_parser):
    if output_parser:
        output = {"entity": [], "star_char": [], "end_char": [], "label_": []}
        for ent in doc.ents:
            output["entity"].append(ent)
            output["star_char"].append(ent.start_char)
            output["end_char"].append(ent.end_char)
            output["label_"].append(ent.label_)
        return output
    else:
        return {(ent.start_char, ent.end_char, ent.label_) for ent in doc.ents}


def lemmatization(doc, output_parser):
    if output_parser:
        output = {"lemma_": []}
        for token in doc:
            output["lemma_"].append(token.lemma_)
        return output
    else:
        return {(token.lemma_) for token in doc}


def dependency_parsing(doc, output_parser):
    if output_parser:
        output = {
            "text": [],
            "dep_": [],
            "head.text": [],
            "head.pos_": [],
            "children": [],
        }
        for token in doc:
            children = str([child for child in token.children])
            output["text"].append(token.text)
            output["dep_"].append(token.dep_)
            output["head.text"].append(token.head.text)
            output["head.pos_"].append(token.head.pos_)
            output["children"].append(children)
        return output
    else:
        return {
            (
                token.text,
                token.dep_,
                token.head.text,
                token.head.pos_,
                str([child for child in token.children]),
            )
            for token in doc
        }


def pos_tagging(doc, output_parser):
    if output_parser:
        output = {
            "text": [],
            "lemma_": [],
            "pos_": [],
            "tag_": [],
            "dep_": [],
            "shape_": [],
            "is_alpha": [],
            "is_stop": [],
        }
        for token in doc:
            output["text"].append(token.text)
            output["lemma_"].append(token.lemma_)
            output["pos_"].append(token.pos_)
            output["tag_"].append(token.tag_)
            output["dep_"].append(token.dep_)
            output["shape_"].append(token.shape_)
            output["is_alpha"].append(token.is_alpha)
            output["is_stop"].append(token.is_stop)
        return output
    else:
        return {
            (
                token.text,
                token.lemma_,
                token.pos_,
                token.tag_,
                token.dep_,
                token.shape_,
                token.is_alpha,
                token.is_stop,
            )
            for token in doc
        }


def morphology(doc, output_parser):
    if output_parser:
        output = {"token": [], "token.morph": []}
        for token in doc:
            output["token"].append(str(token))
            output["token.morph"].append(str(token.morph))
        return output
    else:
        return {(str(token), str(token.morph)) for token in doc}


lingustic_features = {
    "ner": named_entity_recognition,
    "lemmatization": lemmatization,
    "dependency-parsing": dependency_parsing,
    "pos-tag": pos_tagging,
    "morphology": morphology,
}


class SpacyHandler(BaseMLEngine):
    """
    Integration with the spaCy NLP library.
    """

    name = "spacy"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_linguistic_feature = "ner"
        self.default_output_parser = False
        self.linguistic_features = [
            "ner",
            "lemmatization",
            "dependency-parsing",
            "pos-tag",
            "morphology",
        ]

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "spaCy engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

        if len(set(args.keys()) & {"linguistic_feature", "target_column"}) == 0:
            raise Exception(
                "`linguistic_feature` and `target_column` are required for this engine."
            )

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[dict] = None,
    ) -> None:
        model_args = args["using"]
        model_args["target"] = target

        if not args.get("linguistic_feature"):
            args["linguistic_feature"] = self.default_linguistic_feature

        if not args.get("output_parser"):
            args["output_parser"] = self.default_output_parser

        # Loading the model
        nlp = spacy.load("en_core_web_sm")

        # Serialize the model
        bytes_data = nlp.to_bytes()

        # Save the serialized model and its config and model_args to the model storage
        self.model_storage.file_set("model_data", bytes_data)
        self.model_storage.json_set("model_args", model_args)
        self.model_storage.json_set("config", nlp.config)

    def predict(self, df, args=None):
        config = self.model_storage.json_get("config")
        model_args = self.model_storage.json_get("model_args")
        bytes_data = self.model_storage.file_get("model_data")

        # Deserialize the model using the provided config
        lang_cls = spacy.util.get_lang_class(config["nlp"]["lang"])
        nlp = lang_cls.from_config(config)
        nlp.from_bytes(bytes_data)

        column_name = model_args["target_column"]
        linguistic_feature = model_args.get("linguistic_feature")
        output_parser = model_args.get("output_parser")

        predictions = []
        for _, text in df.iterrows():
            doc = nlp(text[column_name])
            if linguistic_feature in lingustic_features:
                entities = lingustic_features[linguistic_feature](doc, output_parser)
                predictions.append(entities)

        # If output_parser is True, spread the prediction values into columns
        if output_parser:
            predictions_df = pd.DataFrame(predictions)
            df = pd.concat([df, predictions_df], axis=1)
        else:
            df[model_args["target"]] = predictions

        return df
