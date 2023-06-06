# Huggingface ML Handler

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?
This handler integrates the Hugging Face Inference API to MindsDB using Hugging-Py-Face, a powerful Python package that provides seamless integration with the Hugging Face Inference API. The source code for the package is available here: https://github.com/MinuraPunchihewa/hugging-py-face

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
This integration is useful for a number of reasons,
- Faster model deployment: Pre-trained models can be quickly deployed for use in applications without having to spend time training the models.
- Greater model variety: Hugging Face offers a large and growing collection of pre-trained models for a wide range of tasks that are made available through this integration.
- Reduced computational costs: Computational costs of training models from scratch can be avoided.

An ideal predictive use case for this integration would be one where you have a large and complex dataset that you want to use to make predictions or classifications, and where you don't have the time or resources to train a model from scratch.

This integration is not suitable for uses cases that require a highly customized machine learning model trained on a specific dataset.

## Are models created with this integration fast and scalable, in general?
In general, yes. However, the free Inference API may be rate limited for heavy use cases. More information about the API can be found here: https://huggingface.co/docs/api-inference/faq. Furthermore, the performance of the Computer Vision and Audio Processing tasks on large datasets will be somewhat slow as a API call needs to be made for each image/audio file to be processed. Future work will include optimizing performance through parallelism and batch jobs, however, this will be supported only for paid plans of the Hugging Face Inference API.

## What are the recommended system specifications for models created with this framework?
Since the models are hosted on by Hugging Face, there is no need for any additional system specifications. The only requirement is to have an internet connection.

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
Users are allowed complete control over the underlying framework by passing parameters via the USING syntax. The parameters are passed to the Hugging Face Inference API, which is used to create the models. The parameters supported for each task are documented here: https://huggingface.co/docs/api-inference/detailed_parameters

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
No, this integration does not offer model explainability or insights via the DESCRIBE syntax.

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No, fine-tuning is not supported. This integration is only for inference using the hosted models of the Hugging Face Inference API.

## Are there any other noteworthy aspects to this handler?
A few other noteworthy aspects of this handler are,
- Flexibility: This integration provides a lot of flexibility in terms of the types of models you can build and the types of data you can work with. A variety of pre-trained models can be used for Natural Language Processing, Computer Vision and Audio Processing.
- Ease of use: Similar to the Hugging Face Inference API itself, this integration is desgined to be easy to use.

## Any directions for future work in subsequent versions of the handler?
Future work includes adding support for other tasks supported by the Hugging Face Inference API and optimizing performance through parallelism and batch jobs.

## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
```sql
-- Create Huggingface engine
CREATE ML_ENGINE hf_api_engine
FROM huggingface_api
USING
  api_key = 'hf_...';

-- Create a model for text classification
CREATE MODEL mindsdb.hf_sentiment_classifier
PREDICT sentiment
USING
  task = 'text-classification',
  column = 'text',
  engine = 'hf_api_engine';

-- Predict sentiment using model
SELECT text, sentiment
FROM mindsdb.hf_sentiment_classifier
WHERE text = 'I like you. I love you.';
```
