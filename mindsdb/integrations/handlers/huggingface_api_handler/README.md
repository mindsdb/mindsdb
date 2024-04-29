---
title: Hugging Face Inference API
sidebarTitle: Hugging Face Inference API
---

This documentation describes the integration of MindsDB with [Hugging Face Inference API](https://huggingface.co/inference-api/serverless).
The integration allows for the deployment of Hugging Face models through Inference API within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Hugging Face Inference API within MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).
3. Obtain the API key for Hugging Face Inference API required to deploy and use Hugging Face models through Inference API within MindsDB. Generate tokens in the `Settings -> Access Tokens` tab of the Hugging Face account.

## Setup

Create an AI engine from the [Hugging Face Inference API handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/huggingface_api_handler).

```sql
CREATE ML_ENGINE huggingface_api_engine
FROM huggingface_api
USING
      huggingface_api_api_key = 'api-key-value';
```

Create a model using `huggingface_api_engine` as an engine.

```sql
CREATE MODEL huggingface_api_model
PREDICT target_column
USING
      engine = 'huggingface_api_engine',     -- engine name as created via CREATE ML_ENGINE
      task = 'task_name',                -- choose one of 'text-classification', 'text-generation', 'question-answering', 'sentence-similarity', 'zero-shot-classification', 'summarization', 'fill-mask', 'image-classification', 'object-detection', 'automatic-speech-recognition', 'audio-classification'
      input_column = 'column_name',      -- column that stores input/question to the model
      labels = ['label 1', 'label 2'];   -- labels used to classify data (used for classification tasks)
```

The following parameters are supported in the `USING` clause of the `CREATE MODEL` statement:

| Parameter          | Required                                     | Description     |
| ------------------ | -------------------------------------------- | --------------- |
| `engine`           | Yes                                          | It is the name of the ML engine created with the `CREATE ML_ENGINE` statement.|
| `task`             | Only if `model_name` is not provided         | It describes a task to be performed.|
| `model_name`       | Only if `task` is not provided               | It specifies a model to be used.|
| `input_column`     | Yes                                          | It is the name of the column that stores input to the model.|
| `endpoint`         | No                                           | It defines the endpoint to use for API calls. If not specified, the hosted Inference API from Hugging Face will be used.|
| `options`          | No                                           | It is a JSON object containing additional options to pass to the API call. More information about the available options for each task can be found [here](https://huggingface.co/docs/api-inference/detailed_parameters).|
| `parameters`       | No                                           | It is a JSON object containing additional parameters to pass to the API call. More information about the available parameters for each task can be found [here](https://huggingface.co/docs/api-inference/detailed_parameters).|
| `context_column`   | Only if `task` is `question-answering`       | It is used for the `question-answering` task to provide context to the question.|
| `input_column2`    | Only if `task` is `sentence-similarity`      | It is used for the `sentence-similarity` task to provide the second input sentence for comparison.|
| `candidate_labels` | Only if `task` is `zero-shot-classification` | It is used for the `zero-shot-classification` task to classify input data according to provided labels.|

## Usage

The following usage examples utilize `huggingface_api_engine` to create a model with the `CREATE MODEL` statement.

Create a model to classify input text as spam or ham.

```sql
CREATE MODEL spam_classifier
PREDICT is_spam
USING
      engine = 'huggingface_api_engine',
      task = 'text-classification',
      column = 'text';
```

Query the model to get predictions.

```sql
SELECT text, is_spam
FROM spam_classifier
WHERE text = 'Subscribe to this channel asap';
```

Here is the output:

```sql
+--------------------------------+---------+
| text                           | is_spam |
+--------------------------------+---------+
| Subscribe to this channel asap | spam    |
+--------------------------------+---------+
```

<Info>

Find more quick examples below:

<AccordionGroup>

<Accordion title="Text Classification">
```sql
CREATE MODEL mindsdb.hf_text_classifier
PREDICT sentiment
USING
  task = 'text-classification',
  engine = 'hf_api_engine',
  input_column = 'text';
```
</Accordion>

<Accordion title="Fill Mask">
```sql
CREATE MODEL mindsdb.hf_fill_mask
PREDICT sequence
USING
  task = 'fill-mask',
  engine = 'hf_api_engine',
  input_column = 'text';
```
</Accordion>

<Accordion title="Summarization">
```sql
CREATE MODEL mindsdb.hf_summarizer
PREDICT summary
USING
  task = 'summarization',
  engine = 'hf_api_engine',
  input_column = 'text';
```
</Accordion>

<Accordion title="Text Generation">
```sql
CREATE MODEL mindsdb.hf_text_generator
PREDICT generated_text
USING
  task = 'text-generation',
  engine = 'hf_api_engine',
  input_column = 'text';
```
</Accordion>

<Accordion title="Question Answering">
```sql
CREATE MODEL mindsdb.hf_question_answerer
PREDICT answer
USING
  task = 'question-answering',
  engine = 'hf_api_engine',
  input_column = 'question',
  context_column = 'context';
```
</Accordion>

<Accordion title="Sentences Similarity">
```sql
CREATE MODEL mindsdb.hf_sentence_similarity
PREDICT similarity
USING
  task = 'sentence-similarity',
  engine = 'hf_api_engine',
  input_column = 'sentence1',
  input_column2 = 'sentence2';
```
</Accordion>

<Accordion title="Zero Shot Classification">
```sql
CREATE MODEL mindsdb.hf_zero_shot_classifier
PREDICT label
USING
  task = 'zero-shot-classification',
  engine = 'hf_api_engine',
  input_column = 'text',
  candidate_labels = ['label1', 'label2', 'label3'];
```
</Accordion>

<Accordion title="Image Classification">
```sql
CREATE MODEL mindsdb.hf_image_classifier
PREDICT label
USING
  task = 'image-classification',
  engine = 'hf_api_engine',
  input_column = 'image_url';
```
</Accordion>

<Accordion title="Object Detection">
```sql
CREATE MODEL mindsdb.hf_object_detector
PREDICT objects
USING
  task = 'object-detection',
  engine = 'hf_api_engine',
  input_column = 'image_url';
```
</Accordion>

<Accordion title="Automatic Speech Recognition">
```sql
CREATE MODEL mindsdb.hf_speech_recognizer
PREDICT transcription
USING
  task = 'automatic-speech-recognition',
  engine = 'hf_api_engine',
  input_column = 'audio_url';
```
</Accordion>

<Accordion title="Audio Classification">
```sql
CREATE MODEL mindsdb.hf_audio_classifier
PREDICT label
USING
  task = 'audio-classification',
  engine = 'hf_api_engine',
  input_column = 'audio_url';
```
</Accordion>

</AccordionGroup>

</Info>

<Tip>

**Next Steps**

Follow [this link](https://docs.mindsdb.com/sql/tutorials/hugging-face-inference-api-examples) to see more use case examples.
</Tip>
