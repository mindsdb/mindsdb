from mindsdb.integrations.handlers.huggingface_handler.finetune import (
    _finetune_cls,
    _finetune_fill_mask,
    _finetune_question_answering,
    _finetune_summarization,
    _finetune_text_generation,
    _finetune_translate,
)

# todo once we have moved predict tasks functions into a separate function
# PREDICT_MAP = {
#             'text-classification': self.predict_text_classification,
#             'zero-shot-classification': self.predict_zero_shot,
#             'translation': self.predict_translation,
#             'summarization': self.predict_summarization,
#             'fill-mask': self.predict_fill_mask
#         }

FINETUNE_MAP = {
    "text-classification": _finetune_cls,
    "zero-shot-classification": _finetune_cls,
    "translation": _finetune_translate,
    "summarization": _finetune_summarization,
    "fill-mask": _finetune_fill_mask,
    "text-generation": _finetune_text_generation,
    "question-answering": _finetune_question_answering,
}
