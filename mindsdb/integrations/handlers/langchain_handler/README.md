### Examples of usage

**Create model for chatbot (conversational mode):**

Openai model
```sql
CREATE MODEL airline_model
	PREDICT answer USING
	engine = "langchain_engine",
	input_column = "question",
	api_key = "<open api key>",
	mode = "conversational",
	user_column = "question",
	assistant_column = "answer",
	model_name = "gpt-3.5-turbo",
	verbose=True,
	prompt_template="Answer the user input in a helpful way";
```

Anyscale provider (defined in 'provider' param)
```sql
CREATE MODEL airline_model
	PREDICT answer 
    USING
	engine = "langchain_engine",
    provider='anyscale',
	model_name = "mistralai/Mistral-7B-Instruct-v0.1",
    api_key = '<anyscale api key>',
    base_url="https://api.endpoints.anyscale.com/v1",
	mode = "conversational",
	user_column = "question",
	assistant_column = "answer",  
	verbose=True,
	prompt_template="Answer the user input in a helpful way";
```

Using mindsdb litellm server
```sql
CREATE MODEL airline_model5
	PREDICT answer 
    USING
	engine = "langchain_engine",
    provider='litellm',
	model_name = "assistant",
    api_key = '<mindsdb api key>',
    base_url="https://ai.dev.mindsdb.com",
	mode = "conversational",
	user_column = "question",
	assistant_column = "answer",    
	verbose=True,
	prompt_template="Answer the user input in a helpful way";
```
