from integrations.handlers.sentence_transformer_handler.settings import (
    load_embeddings_model,
)


def get_embeddings(args):

    model = load_embeddings_model(args["embeddings_model_name"])

    model.encode()
