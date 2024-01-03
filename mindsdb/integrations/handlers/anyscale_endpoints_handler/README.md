# Anyscale Endpoints Handler

This integration allows you to connect your AnyscaleEndpoints models with MindsDB as AI tables through the `AnyscaleEndpoints` ML engine.


### Implementation

The implementation is based on the engine for the OpenAI API, as Anyscale conforms to it.

There are a few notable differences, though:
1. The supported models are completely different. 
   1. All models supported by AnyscaleEndpoints are open source. A full list can be found [here](https://app.endpoints.anyscale.com/docs) for inference-only, under section "Supported models".
   2. For fine-tuning, not every model is supported. You can find a list [here](https://app.endpoints.anyscale.com/docs), under section "Fine Tuning - Supported models".
   3. Please check both lists regularly, as they are subject to change. If you try to fine-tune a model that is not supported, you will get a warning and subsequently an error from the Anyscale endpoint.
2. This integration only offers chat-based text completion models, either for "normal" text or specialized for code.
3. When providing a description, this integration returns the respective HuggingFace model card.
4. Fine-tuning requires that your dataset complies with the chat format. That is, each row should contain a `context` and a `role`. The `context` is the text that is the message in the chat, and the `role` is who authored it (`system`, `user` or `assistant`, where the last one is the model). For more information, please check the fine tuning guide in the Anyscale Endpoints [docs](https://app.endpoints.anyscale.com/docs).

### Base URL
The base URL for this API is `https://api.endpoints.anyscale.com/v1`.

## Inference example

Here is an example to produce sentiment analysis predictions on some text:

```sql
CREATE ML_ENGINE anyscale_endpoints FROM anyscale_endpoints USING api_key = 'your-api-key';
    
CREATE MODEL anyscale_endpoints_sentiment_model
PREDICT sentiment
USING
engine = 'anyscale_endpoints',
model_name = 'mistralai/Mistral-7B-Instruct-v0.1',
prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
    
SELECT sentiment 
FROM anyscale_endpoints_sentiment_model
WHERE text = 'I love machine learning!';
```
