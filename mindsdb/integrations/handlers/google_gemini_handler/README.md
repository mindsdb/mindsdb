---
title: Google Gemini
sidebarTitle: Google Gemini
---

This documentation describes the integration of MindsDB with [Google Gemini](<link>), a generative artificial intelligence model developed by Google. The integration allows for the deployment of Google Gemini models within MindsDB, providing the models with access to data from various data sources.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To use Google Gemini within MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).
3. Obtain the Google Gemini API key required to deploy and use Google Gemini models within MindsDB. Follow the [instructions for obtaining the API key](https://ai.google.dev/gemini-api/docs/api-key).

## Setup

Create an AI engine from the [Google Gemini handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/google_gemini_handler).

```sql
CREATE ML_ENGINE google_gemini_engine
FROM google_gemini
USING
      api_key = 'api-key-value';
```

## Example Usage

### Create Gemini Pro Model (Prompt-Template)

```sql
CREATE MODEL gem_p
PREDICT answer
USING
    engine = 'google_gemini_engine',
    prompt_template = 'Product Description: {{description}}. Question: {{question}}. Answer:',
    model_name = 'gemini-pro';
```

```sql
SELECT answer
FROM gem_p
WHERE description = "
What is Rabbit R1?
The Rabbit R1 is a pocket-sized AI device that promises a simpler and more intuitive way to interact with technology. Instead of being app-driven, the device relies on an AI model called LAMB (large action model) to understand your instructions and complete tasks autonomously.
The device has a bright orange body, and is small and lightweight with a touchscreen, scroll wheel, and a talk button. There is also a rotating camera that functions as eyes of the device.

The Rabbit R1 runs on its own operating system, called the Rabbit OS, that eliminates the need for app stores and downloads, requiring only natural language voice input to navigate. The initial version supports integration with the likes of Uber, Spotify, and Amazon, with the AI able to train and learn using other apps in the future.
"
AND question = 'What are some key feature bullet points of this product?';
```

### Create Gemini Contextual Model (Column-based)

```sql
CREATE MODEL gemini_c
PREDICT answer
USING
    engine = 'google_gemini_engine',
    question_column = 'question',
    context_column = 'context',
    model_name = "gemini-pro";
```

```sql
SELECT answer
FROM gem_qc
WHERE context = "Ashoka the Great was an Indian emperor of the Maurya Dynasty who ruled from 268 to 232 BCE. He is regarded as one of India's greatest emperors, known for his extensive empire, his efforts to spread Buddhism, and his commitment to non-violence and peaceful coexistence."
AND question = 'Ashoka was from which dynasty?';
```

### Vision Mode Query

```sql
CREATE MODEL gem_v
PREDICT answer
USING
    engine = 'google_gemini_engine',
    mode = 'vision',
    img_url = 'url',
    ctx_column = 'context';
```

```sql
SELECT *
FROM gem_v
WHERE url = 'https://images.unsplash.com/photo-1589762738975-a6773160c7d7?q=80&w=1374&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D'
AND context = 'Is this man a superhuman?';
```

### Embedding Mode

```sql
CREATE MODEL gem_e
PREDICT answer
USING
    engine = 'google_gemini_engine',
    mode = 'embedding',
    model_name = 'models/embedding-001',
    question_column = 'question',
    context_column = 'context',
    title_column = 'title'; -- OPTIONAL
```

```sql
SELECT question, answer
FROM gem_e
WHERE question = 'How many moons are there in the solar system?'
USING
    type = 'document';
```

### JSON-Struct Mode

```sql
CREATE MODEL product_extract_json
PREDICT json
USING
    engine = 'google_gemini_engine',
    json_struct = {
        'product_name': 'name',
        'product_category': 'category',
        'product_price': 'price'
    },
    input_text = 'description';
```

```sql
SELECT json
FROM product_extract_json
WHERE description = "
What is Rabbit R1?
The Rabbit R1 is a pocket-sized AI device that promises a simpler and more intuitive way to interact with technology. Instead of being app-driven, the device relies on an AI model called LAMB (large action model) to understand your instructions and complete tasks autonomously.
The device has a bright orange body, and is small and lightweight with a touchscreen, scroll wheel, and a talk button. There is also a rotating camera that functions as eyes of the device. IT provides all of this just for 300 dollars.

The Rabbit R1 runs on its own operating system, called the Rabbit OS, that eliminates the need for app stores and downloads, requiring only natural language voice input to navigate. The initial version supports integration with the likes of Uber, Spotify, and Amazon, with the AI able to train and learn using other apps in the future.
";
```

**Output**

![image](https://github.com/mindsdb/mindsdb/assets/75653580/aad51d3f-4458-4bcc-b4a3-07983496d2fe)


### Create a model to generate text completions with the Gemini Pro model for your existing text data.

```sql
CREATE MODEL google_gemini_model
PREDICT answer
USING
    engine = 'google_gemini_engine',
    column = 'question',
    model = 'gemini-pro';
```

### Query the model to get predictions.

```sql
SELECT question, answer
FROM google_gemini_model
WHERE question = 'How are you?';
```

### Query for batch predictions

```sql
SELECT t.question, m.answer
FROM google_gemini_model AS m
JOIN data_table AS t;
```

### Describe Gemini Pro Model Metadata

```sql
DESCRIBE MODEL `MODEL_NAME`.metadata;
```

<Tip>
**Next Steps**

Go to the [Use Cases](/use-cases/overview) section to see more examples.
</Tip>