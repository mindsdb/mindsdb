# HuggingFace API Handler

The HuggingFace API handler for MindsDB provides an interface to interact with the HuggingFace Inference API.

## About the HuggingFace Inference API
Easily integrate NLP, audio and computer vision models deployed for inference via simple API calls. Harness the power of machine learning while staying out of MLOps!
<br>
https://huggingface.co/inference-api

## HuggingFace API Handler Implementation

This handler was implemented using [Hugging-Py-Face](https://github.com/MinuraPunchihewa/hugging-py-face), a powerful Python package that provides seamless integration with the Hugging Face Inference API.

## Creating an ML Engine

The first step to make use of this handler is to create an ML Engine. This can be done using the following syntax,
```sql
CREATE ML_ENGINE hf_api_engine
FROM huggingface_api
USING
  api_key = '<YOUR_API_KEY>';
```

## Creating Models

Now, you can use this ML Engine to create Models for the different tasks supported by the handler. 

When executing the `CREATE MODEL` statement, the following parameters are supported in the `USING` clause of the query:
- `engine`: The name of the ML Engine to use. This is a required parameter.
- `api_key`: The HuggingFace API key to use for authentication. This is a required parameter.
- `task`: The task to perform. This is an optional parameter. If not specified, the `model_name` parameter must be specified instead and the handler will automatically infer the task.
- `model_name`: The name of the model to use. This is an optional parameter. If not specified, the `task` parameter must be specified instead and the handler will automatically use the recommended model for the task.
- `input_column`: The name of the column to use as input to the model. This is a required parameter.
- `endpoint`: The endpoint to use for the API call. This is an optional parameter. If not specified, the hosted inference API from HuggingFace will be used.
- `options`: A JSON object containing additional options to pass to the API call. This is an optional parameter. More information about the available options for each task can be found [here](https://huggingface.co/docs/api-inference/detailed_parameters).
- `parameters`: A JSON object containing additional parameters to pass to the API call. This is an optional parameter. More information about the available parameters for each task can be found [here](https://huggingface.co/docs/api-inference/detailed_parameters).

In addition to the above, there are a few task-specific parameters that the handler supports. These will be introduced in the following sections.`

Given below are examples of creating Models for each of the supported tasks.

### Text Classification
```sql
CREATE MODEL mindsdb.hf_text_classifier
PREDICT sentiment
USING
  task = 'text-classification',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'text'
```

### Fill Mask
```sql
CREATE MODEL mindsdb.hf_fill_mask
PREDICT sequence
USING
  task = 'fill-mask',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'text'
```

### Summarization
```sql
CREATE MODEL mindsdb.hf_summarizer
PREDICT summary
USING
  task = 'summarization',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'text'
```

### Text Generation
```sql
CREATE MODEL mindsdb.hf_text_generator
PREDICT generated_text
USING
  task = 'text-generation',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'text'
```

### Question Answering
```sql
CREATE MODEL mindsdb.hf_question_answerer
PREDICT answer
USING
  task = 'question-answering',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'question',
  context_column = 'context'
```

The `context_column` parameter is specific to the Question Answering task and is used to specify the column containing the context for the question.

### Sentences Similarity
```sql
CREATE MODEL mindsdb.hf_sentence_similarity
PREDICT similarity
USING
  task = 'sentence-similarity',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'sentence1',
  input_column2 = 'sentence2'
```

The `input_column2` parameter is specific to the Sentence Similarity task and is used to specify the column containing the second sentence to compare.

### Zero Shot Classification
```sql
CREATE MODEL mindsdb.hf_zero_shot_classifier
PREDICT label
USING
  task = 'zero-shot-classification',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'text',
  candidate_labels = ['label1', 'label2', 'label3']
```

The `candidate_labels` parameter is specific to the Zero Shot Classification task and is used to specify the candidate labels to use for classification.

### Image Classification
```sql
CREATE MODEL mindsdb.hf_image_classifier
PREDICT label
USING
  task = 'image-classification',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'image_url'
```

### Object Detection
```sql
CREATE MODEL mindsdb.hf_object_detector
PREDICT objects
USING
  task = 'object-detection',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'image_url'
```

### Automatic Speech Recognition
```sql
CREATE MODEL mindsdb.hf_speech_recognizer
PREDICT transcription
USING
  task = 'automatic-speech-recognition',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'audio_url'
```

### Audio Classification
```sql
CREATE MODEL mindsdb.hf_audio_classifier
PREDICT label
USING
  task = 'audio-classification',
  engine = 'hf_api_engine',
  api_key = '<YOUR_API_KEY>',
  input_column = 'audio_url'
```

## Making Predictions

### Single Prediction
A single prediction can be made by running SELECT statements and passing inputs as part of the WHERE clause. The following example shows how to make a single prediction using the Text Classification model created in the previous section.
```sql
SELECT text, sentiment
FROM mindsdb.hf_text_classifier
WHERE text = 'I love MindsDB';
```

### Batch Prediction
