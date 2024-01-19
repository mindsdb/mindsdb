
# Replicate Handler

This handler was implemented using the `replicate` library that is provided by Replicate.

The required arguments to establish a connection are,

* model_name: Model name which you want to access in MindsDB. e.g 'air-forever/kandinsky-2'
* version: version hash/id which you want to use in MindsDB.
* api_key: API key from Replicate Platform you can found [here](https://replicate.com/account/api-tokens).


## Usage

To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).


To establish the connection and create a model in MindsDB, use the following syntax:

```sql

CREATE MODEL audio_ai
PREDICT audio
USING
    engine = 'replicate',
    model_name= 'afiaka87/tortoise-tts',
    version ='e9658de4b325863c4fcdc12d94bb7c9b54cbfe351b7ca1b36860008172b91c71',
    api_key = 'r8_BpO.........................';
```

> Note before using Replicate you will need to Authenticate by setting your token in an environment variable `REPLICATE_API_TOKEN`. If you are using `pip` run `export REPLICATE_API_TOKEN=YOUR TOKEN` or `Docker` with `docker run -e REPLICATE_API_TOKEN=YOUR TOKEN -p 47334:47334 -p 47335:47335 mindsdb/mindsdb`

## Models Examples

* [text2video](https://docs.mindsdb.com/integrations/ai-engines/replicate-text2video)
* [audio](https://docs.mindsdb.com/integrations/ai-engines/replicate-audio)
* [img2text](https://docs.mindsdb.com/integrations/ai-engines/replicate-img2text)
* [text2image](https://docs.mindsdb.com/integrations/ai-engines/replicate-text2img)