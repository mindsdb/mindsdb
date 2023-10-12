# Replicate Handler

This is the implementation of the Replicate handler for MindsDB.

## Replicate
Replicate is a platform and tool that aims to make it easier to work with machine learning models. It provides a library of open-source machine learning models that can be run in the cloud. Replicate allows users to deploy their own machine learning models at scale by providing infrastructure and automatic generation of API servers.


## Implementation
This handler was implemented using the `replicate` library that is provided by Replicate.

The required arguments to establish a connection are,
- 
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

You can use the DESCRIBE PREDICTOR query to see the available parameters that you can specify to customize your predictions:
```sql
DESCRIBE PREDICTOR mindsdb.audio_ai.features;
```
### Output
```sql
+--------------+---------+-----------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inputs       | type    | default                                                                                       | description                                                                                                                                                                             |
+--------------+---------+-----------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| seed         | integer | 0                                                                                             | Random seed which can be used to reproduce results.                                                                                                                                     |
| text         | string  | The expressiveness of autoregressive transformers is literally nuts! I absolutely adore them. | Text to speak.                                                                                                                                                                          |
| preset       | -       | fast                                                                                          | Which voice preset to use. See the documentation for more information.                                                                                                                  |
| voice_a      | -       | random                                                                                        | Selects the voice to use for generation. Use `random` to select a random voice. Use `custom_voice` to use a custom voice.                                                               |
| voice_b      | -       | disabled                                                                                      | (Optional) Create new voice from averaging the latents for `voice_a`, `voice_b` and `voice_c`. Use `disabled` to disable voice mixing.                                                  |
| voice_c      | -       | disabled                                                                                      | (Optional) Create new voice from averaging the latents for `voice_a`, `voice_b` and `voice_c`. Use `disabled` to disable voice mixing.                                                  |
| cvvp_amount  | number  | 0                                                                                             | How much the CVVP model should influence the output. Increasing this can in some cases reduce the likelyhood of multiple speakers. Defaults to 0 (disabled)                             |
| custom_voice | string  | -                                                                                             | (Optional) Create a custom voice based on an mp3 file of a speaker. Audio should be at least 15 seconds, only contain one speaker, and be in mp3 format. Overrides the `voice_a` input. |
+--------------+---------+-----------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Now, you can use the established connection to query your ML Model as follows:

# Audio Generation



## Custom Audio Cloning

```sql
SELECT * FROM audio_ai
WHERE 
    text = "This is breaking news that first humans have landed on Mars, and they have found something very unusual there. By the way, this is the future."
USING 
    voice_a = 'custom_voice',
    custom_voice = 'https://123bien.com/wp-content/uploads/2019/05/i-want-to-work-2.mp3';
```
OUTPUT
```sql
+------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
| audio                                                                                          | text                                                                                                                                                 |
+------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
| https://replicate.delivery/pbxt/ffOCXeL4fa5yekAL4ybfFiJBbqENSEjhSLpA2zp1ElsBxxhSE/tortoise.mp3 | This is breaking news that first human are landed on mars and they find something very unusal their ehich is not yet out, by the way this is future  |
+------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------+
```
If above predicted url don't work , then use [this](./assets/cloned_audio.mp3)

## Audio Generation 

```sql
SELECT * FROM audio_ai
WHERE 
    text = "An image captured by NASA's Mars Curiosity Rover shows a faint figure of a woman against the desert landscape of Mars. If you take a closer look, it will seem that the lady is standing on a cliff overlooking the vast undulating expanse. She seems to wear a long cloak and has long hair."
USING 
    voice_a = 'random';
```

### OUTPUT
```sql
+---------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| audio                                                                                       | text                                                                                                                                                                                                                                                                                         |
+---------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| https://replicate.delivery/pbxt/EQj2DtBn5fxVA6P97GfPYthmgd0I3VaOEGFnweE4hvl5BPUiA/audio.wav | An image captured by NASA's Mars Curiosity Rover shows a faint figure of a woman against the desert landscape of Mars. If you take a closer look, it will seem that the lady is standing on a cliff overlooking the vast undulating expanse. She seems to wear a long cloak and has long hair. |
+---------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Above predicted url will work , therfore use [this](./assets/generated_audio.wav).

This is just an one model used in this example there are more with vast variation and usescases.
Also there is no limit to imagination, how can you use this.

- IMPORTANT NOTE: PREDICTED **URL** will only work for **24 hours** after prediction.
> Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
