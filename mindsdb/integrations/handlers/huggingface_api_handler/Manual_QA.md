# Welcome to the MindsDB Manual QA Testing for Hugging Face Inference API Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Hugging Face Inference API Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE ML_ENGINE**

```
CREATE ML_ENGINE <engine_name>
FROM huggingface_api
USING api_key = '<hf_api_key>';
```

![CREATE_ML_ENGINE](Image URL of the screenshot)

**2. Testing CREATE MODEL**

```
CREATE MODEL <model_name>
PREDICT <target_column>
USING
  engine = 'hf_inference_api',
  task = '<task_name>',
  column = '<input_column>';
```

![CREATE_MODEL](Image URL of the screenshot)

**3. Testing SELECT FROM MODEL**

```
COMMAND THAT YOU RAN TO DO A SELECT FROM.
```

![SELECT_FROM](Image URL of the screenshot)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---