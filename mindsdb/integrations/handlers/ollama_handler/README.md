# Ollama handler 

## Briefly describe the ML framework this handler integrates with MindsDB, and how?
[Ollama](https://ollama.ai/) is a project that enables easy local deployment of Large Language Models (LLMs). All models supported by Ollama are available in MindsDB through this integration.

For now, this integration will only work in MacOS, with Linux and Windows to come later.

Call this handler by
`USING ENGINE="ollama"`, you can see a full example at the end of this readme.

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
Locally deployed LLMs can be desirable for a wide variety of reasons. In this case, data privacy, developer feedback-loop speed and inference cost reduction can be powerful reasons to opt for a local LLM.

Ideal predictive use cases, as in other LLM-focused integrations (e.g. OpenAI, Anthropic, Cohere), will be anything involving language understanding and generation, including but not limited to:
- zero-shot text classification
- sentiment analysis
- question answering
- summarization
- translation

Some current limitations of local LLMs:
- overall weaker performance (ranging from "somewhat" to "a lot") than commercial cloud-based LLMs, particularly GPT-4. Please study options carefully and benchmark thoroughly to ensure your LLM is at the right level of performance for your use case before deploying to production.
- steep entry barrier due to required hardware specs (macOS only, M1 chip or greater, a lot of RAM depending on model size)

## Are models created with this integration fast and scalable, in general?
Model training is not required, as these are pretrained models. 

Inference is generally fast, however stream generation is not supported at this time in MindsDB, so completions are only returned once the model has finished generating the entire sequence.

## What are the recommended system specifications?

* A macOS machine, M1 chip or greater. 
* A working Ollama installation. For instructions refer to their [webpage](https://ollama.ai). This step should be really simple.
* For 7B models, at least 8GB RAM is recommended. 
* For 13B models, at least 16GB RAM is recommended. 
* For 70B models, at least 64GB RAM is recommended.

More information [here](https://ollama.ai/library/llama2). Minimum specs can vary depending on the model.

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
The prompt template can be overridden at prediction time, e.g.:

```sql
-- example: override template at prediction time
SELECT text, completion
FROM my_llama2
WHERE text = 'hi there!';
USING 
prompt_template = 'Answer using exactly five words: {{text}}:';
```

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
It replicates the information exposed by the Ollama API, plus a few additional MindsDB-specific fields.

Supported commands are:
1. `DESCRIBE ollama_model;`
2. `DESCRIBE ollama_model.model;`
3. `DESCRIBE ollama_model.features;`

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not at this time.

## Any directions for future work in subsequent versions of the handler?
A few are commented in the code:
1. add support for overriding modelfile params (e.g. temperature)
2. add support for storing `context` short conversational memory
3. actually store all model artifacts in the engine storage, instead of the internal Ollama mechanism. This may require upstream changes, though.

## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
```sql
CREATE ML_ENGINE ollama FROM ollama;

CREATE MODEL my_llama2
PREDICT completion
USING
model_name = 'llama2',
engine = 'ollama';

DESCRIBE my_llama2.model;
DESCRIBE my_llama2.features;

SELECT text, completion
FROM my_llama2
WHERE text = 'hi there!';
```