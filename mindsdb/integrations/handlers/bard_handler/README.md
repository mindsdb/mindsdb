# Bard Handler
The Bard ML handler integrates the Bard library from Google's Generative AI with MindsDB. You can use it to generate text completions with the Gemini Pro model for your existing text data.

## Google Generative AI (Bard)
Google Generative AI is a library that provides access to powerful language models for text generation. The Bard library, used in this handler, offers various models for generating text. More information about the Google Generative AI library can be found [here](https://github.com/GoogleCloudPlatform/generative-ai).

*Note:* Ensure you have the necessary API key for accessing the Bard library.

# Example Usage

Create a ML Engine with the new `bard` engine.

```sql
CREATE ML_ENGINE Bard_ML_Engine
FROM bard
USING
  api_key = 'your_api_key';
```

```sql
CREATE MODEL mindsdb.bard_test
PREDICT answer
USING
  column = 'question',
  engine = 'bard',
  api_key = 'your_api_key',
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

# Bard Handler Python Code Explanation

The provided Python code defines a `BardHandler` class, which is a subclass of `BaseMLEngine` in MindsDB's integration library. Here's a breakdown of its key components:

- **Initialization:**
  - The `name` attribute is set to "bard" to identify this handler.
  - Default and supported chat models are defined, and the generative property is set to `True`.

- **Model Creation:**
  - The `create` method stores model arguments in the model's JSON storage.

- **Prediction:**
  - The `predict` method retrieves model arguments and the Bard API key.
  - It configures the Bard library with the provided API key.
  - Predictions are made for each input text in the specified column.
  - The results are returned as a DataFrame with the target column containing the generated completions.

- **API Key Handling:**
  - The `_get_bard_api_key` method prioritizes API keys based on different sources, ensuring flexibility in key retrieval.

- **Answer Prediction:**
  - The `predict_answer` method generates text completions using the Bard library based on the provided prompts and input text.

This handler allows seamless integration of the Bard library for text generation within the MindsDB ecosystem.