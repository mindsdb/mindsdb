# Google Gemini Handler
The Google Gemini ML handler integrates the google-gen-ai library from Google's Generative AI with MindsDB. You can use it to generate text completions with the Gemini Pro model for your existing text data.

## Google Generative AI (Google Gemini)
Google Generative AI is a library that provides access to powerful language models for text generation. More information about the Google Generative AI library can be found [here](https://github.com/GoogleCloudPlatform/generative-ai).

*Note:* Ensure you have the necessary API key for accessing the Google gen AI library. You can get your API key at https://makersuite.google.com/. 

>> This Handler requires python>=3.9 to work
>> (Default model_name is **gemini-pro** )

# Example Usage

#### Create Gemini ML Engine
```sql
CREATE ML_ENGINE g
FROM gemini
USING
    api_key = 'AI-i5-c001';
```


#### Create Gemini Pro Model (Prompt-Template)
```sql
CREATE MODEL gem_p
PREDICT answer
USING
    engine = 'g',
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
AND question = 'Given me bullet pointed features of product ?';
```



#### Create Gemini Contextual Model (Column-based)
```sql
CREATE MODEL gemini_c
PREDICT answer
USING
    engine = 'g',
    question_column = 'question',
    context_column = 'context',
    model_name="gemini-pro";
```
```sql
SELECT answer
FROM gem_qc
WHERE context = "Ashoka the Great was an Indian emperor of the Maurya Dynasty who ruled from 268 to 232 BCE. He is regarded as one of India's greatest emperors, known for his extensive empire, his efforts to spread Buddhism, and his commitment to non-violence and peaceful coexistence."
AND question = 'Ashoka was from which dynasty?';
```


#### Vision Mode Query
```sql
CREATE MODEL gem_v
PREDICT answer
USING
  engine = 'g',
  mode = 'vision',
  img_url = 'url',
  ctx_column = 'context';
```

```sql
SELECT *
FROM gem_v
WHERE  url = 'https://images.unsplash.com/photo-1589762738975-a6773160c7d7?q=80&w=1374&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D'
AND context='Does this man is superhuman?';
```

#### Embedding Mode 
```sql
CREATE MODEL gem_e
PREDICT answer
USING
  engine = 'g',
  mode = 'embedding',
  model_name = 'models/embedding-001',    --- default for embedding mode
  question_column = 'question',
  context_column = 'context',
  title_column = 'title';     --OPTIONAL
```
>> (**title_column** is optional and default model_name for embedding mode is **models/embedding-001'**)

```sql
SELECT  question, answer
FROM gem_e
WHERE  question = 'How many moon exist in the solar system?'
USING 
  type='document';
```
>>(Use ** type='document'** for storing vectors for later searching and **type='query'** for searching in already created vectors.)

#### Describe Gemini Pro Model Metadata
```sql
DESCRIBE MODEL `MODEL_NAME`.metadata;
```