# Bard Handler
The Bard ML handler integrates the google-gen-ai library from Google's Generative AI with MindsDB. You can use it to generate text completions with the Gemini Pro model for your existing text data.

## Google Generative AI (Bard)
Google Generative AI is a library that provides access to powerful language models for text generation. More information about the Google Generative AI library can be found [here](https://github.com/GoogleCloudPlatform/generative-ai).

*Note:* Ensure you have the necessary API key for accessing the Google gen AI library. You can get your API key at https://makersuite.google.com/. 

# Example Usage

Create a ML Engine with the new `bard` engine.

```sql
CREATE ML_ENGINE Bard_ML_Engine
FROM bard
USING
  api_key = 'cloud_api_key';
```

```sql
CREATE MODEL mindsdb.bard_test
PREDICT answer
USING
  column = 'question',
  engine = 'bard',
  api_key = 'cloud_api_key',
  model = 'gemini-pro'
```

```sql
SELECT question, answer
FROM mindsdb.bard_test
WHERE question = 'What is the meaning of life?';
```

```sql
SELECT t.question, m.answer
FROM mindsdb.bard_test as m
JOIN files.question_table as t;
```
