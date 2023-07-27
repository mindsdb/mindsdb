# Replicate Handler

This is the implementation of the Replicate handler for MindsDB.

## Replicate
Replicate is a platform and tool that aims to make it easier to work with machine learning models. It provides a library of open-source machine learning models that can be run in the cloud. Replicate allows users to deploy their own machine learning models at scale by providing infrastructure and automatic generation of API servers.


## Implementation
This handler was implemented using the `replicate` library that is provided by Amazon Web Services.

The required arguments to establish a connection are,
- 
* model_name: Model name which you want to access in MindsDB. e.g 'air-forever/kandinsky-2'
* version: version hash/id which you want to use in MindsDB.
* api_key: API key from Replicate Platform you can found [here](https://replicate.com/account/api-tokens).


## Usage
To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).

Sure, I've reformatted the nested `<details>` and `<summary>` tags. Here's the corrected version:


## Audio Generation


<details>
<summary>Create Model</summary>

```sql
CREATE MODEL audio_ai
PREDICT audio
USING
    engine = 'replicate',
    model_name= 'afiaka87/tortoise-tts',
    version ='e9658de4b325863c4fcdc12d94bb7c9b54cbfe351b7ca1b36860008172b91c71',
    api_key = 'r8_BpO.........................';
```

</details>

<details>
<summary>Describe Model</summary>

You can use the `DESCRIBE PREDICTOR` query to see the available parameters that you can specify to customize your predictions:

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
| cvvp_amount  | number  | 0                                                                                             | How much the CVVP model should influence the output. Increasing this can in some cases reduce the likelihood of multiple speakers. Defaults to 0 (disabled)                             |
| custom_voice | string  | -                                                                                             | (Optional) Create a custom voice based on an mp3 file of a speaker. Audio should be at least 15 seconds, only contain one speaker, and be in mp3 format. Overrides the `voice_a` input. |
+--------------+---------+-----------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

</details>

<details>
<summary>Custom Audio Cloning</summary>

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
If the above predicted URL doesn't work, then use [this link](./assets/cloned_audio.mp3).

</details>

<details>
<summary>Audio Generation</summary>

```sql
SELECT * FROM audio_ai
WHERE 
    text = "An image captured by NASA's Mars Curiosity Rover shows a faint figure of a woman against the desert landscape of Mars. If you take a closer look, it will seem that the lady is standing on a cliff overlooking the vast undulating expanse. She seems to wear a long cloak and has long hair."
USING 
    voice_a = 'random';
```

### OUTPUT

```sql
+---------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| audio                                                                                       | text                                                                                                                                                                                                                                                                                         |
+---------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| https://replicate.delivery/pbxt/EQj2DtBn5fxVA6P97GfPYthmgd0I3VaOEGFnweE4hvl5BPUiA/audio.wav | An image captured by NASA's Mars Curiosity Rover shows a faint figure of a woman against the desert landscape of Mars. If you take a closer look, it will seem that the lady is standing on a cliff overlooking the vast undulating expanse. She seems to wear a long cloak and has long hair. |
+---------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Above predicted URL will work; therefore, you can use [this link](./assets/generated_audio.wav).

</details>
</details>

## Image2Text
<details>
<summary>Create Model</summary>

```sql
CREATE MODEL blip
PREDICT text
USING
    engine = '

replicate',
    model_name= 'salesforce/blip',
    version ='2e1dddc8621f72155f24cf2e0adbde548458d3cab9f00c0139eea840d0ac4746',
    api_key = 'r8_BpO.........................';
```
</details>
<details>
<summary>Describe Model</summary>
You can use the `DESCRIBE PREDICTOR` query to see the available parameters that you can specify to customize your predictions:
  
```sql
DESCRIBE PREDICTOR mindsdb.blip.features;
```
### Output
```sql
+----------+------------------+-----------------------------------------------------------------------+--------+
| inputs   | default          | description                                                           | type   |
+----------+------------------+-----------------------------------------------------------------------+--------+
| task     | image_captioning | Choose a task.                                                        | -      |
| image    | -                | Input image                                                           | string |
| caption  | -                | Type caption for the input image for image text matching task.        | string |
| question | -                | Type question for the input image for visual question answering task. | string |
+----------+------------------+-----------------------------------------------------------------------+--------+
```


</details>

<details>
<summary>Visual Question Answering</summary>

Now, you can use the established connection to query your ML Model as follows:

```sql
SELECT *
FROM mindsdb.blip
WHERE image="https://images.unsplash.com/photo-1686847266385-a32745169de4"
AND question="Is there a lion in the image?"
USING 
task="visual_question_answering";
```
### Output
```sql
+------------+--------------------------------------------------------------+-------------------------+
| text        | image                                                        | question                |
+------------+--------------------------------------------------------------+-------------------------+
| Answer: no | https://images.unsplash.com/photo-1686847266385-a32745169de4 | Is there a lion in the image? |
+------------+--------------------------------------------------------------+-------------------------+
```

</details>

<details>
<summary>Image Captioning</summary>

```sql
SELECT *
FROM mindsdb.blip
WHERE image="https://images.unsplash.com/photo-1686847266385-a32745169de4";
```

### Output
```sql
+---------------------------------------------------+--------------------------------------------------------------+
| text                                               | image                                                        |
+---------------------------------------------------+--------------------------------------------------------------+
| Caption: a bird is sitting on the back of a horse | https://images.unsplash.com/photo-1686847266385-a32745169de4 |
+---------------------------------------------------+--------------------------------------------------------------+
```

</details>

<details>
<summary>Image Text Matching</summary>

```sql 
SELECT *
FROM mindsdb.blip
WHERE image="https://images.unsplash.com/photo-1686847266385-a32745169de4"
AND caption="Bird having horse Riding"
USING 
task="image_text_matching";
```

OUTPUT
```sql
+-----------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+--------------------------+
| text                                                                                                                               | image                                                        | caption                  |
+-----------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+--------------------------+
| The image and text are matched with a probability of 0.7730.
The image feature and text feature have a cosine similarity of 0.3615. | https://images.unsplash.com/photo-1686847266385-a32745169de4 | Bird having horse Riding |
+-----------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+--------------------------+
```

</details>



## LLM
<details>
<summary>Create Model</summary>

```sql
CREATE MODEL vicuna_13b
PREDICT output
USING
    engine = 'replicate',
    model_name= 'replicate/vicuna_13b',
    model_type='LLM',
    version ='6282abe6a492de4145d7bb601023762212f9ddbbe78278bd6771c8b3b2f2a13b',
    api_key = 'r8_HEH............';
```
</details>
<details>
<summary>Describe Model</summary>
You can use the `DESCRIBE PREDICTOR` query to see the available parameters that you can specify to customize your predictions:
  
```sql
DESCRIBE PREDICTOR mindsdb.vicuna_13b.features;
```
### Output
```sql
+--------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------+
| inputs             | type    | default | description                                                                                                                           |
+--------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------+
| seed               | integer | -1      | Seed for random number generator, for reproducibility                                                                                 |
| debug              | boolean | False   | provide debugging output in logs                                                                                                      |
| top_p              | number  | 1       | When decoding text, samples from the top p percentage of most likely tokens; lower to ignore less likely tokens                       |
| prompt             | string  | -       | Prompt to send to Llama.                                                                                                              |
| max_length         | integer | 500     | Maximum number of tokens to generate. A word is generally 2-3 tokens                                                                  |
| temperature        | number  | 0.75    | Adjusts randomness of outputs, greater than 1 is random and 0 is deterministic, 0.75 is a good starting value.                        |
| repetition_penalty | number  | 1       | Penalty for repeated words in generated text; 1 is no penalty, values greater than 1 discourage repetition, less than 1 encourage it. |
+--------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------+
```

</details>

<details>
<summary>Query LLM Model</summary>

Now, you can use the established connection to query your ML Model for a humorous poem on Open Source:

```sql
SELECT *
FROM vicuna_13b
WHERE prompt='Write a humorous poem on Open Source'
USING
 max_length=200,
 temperature=0.75;
```
### Output
```sql
+--------------------------------------------------------------+----------------------------------------+
| output                                                       | prompt                                 |
+--------------------------------------------------------------+----------------------------------------+
| Opensource software, oh how we love thee                     | Write a humorous poem on Open Source   |  
| With bugs and glitches, oh so free                           |                                        |  
| You bring us laughter and joy each day                       |                                        |  
| And we'll never have to pay                                  |                                        |
|                                                              |                                        |  
|                                                              |                                        |  
| The licence is open, the code is there                       |                                        |  
| For all to see and share in cheer                            |                                        |  
| You bring us together, from far and wide                     |                                        |  
| To work on projects, side by side                            |                                        |  
|                                                              |                                        |  
| With open source, there's no end                             |                                        |  
| To the code we can bend                                      |                                        |  
| We can change it, mold it, make it our own                   |                                        |  
| And create something truly great, or really strange          |                                        |  
|                                                              |                                        |  
| So here's to open source, the future is bright               |                                        |  
| With code that's free, and with all of our might             |                                        |  
| We"ll make the future shine, with technology fine            |                                        |  
| And open source will always be our shining line              |                                        |  
+--------------------------------------------------------------+----------------------------------------+
```

</details>

## Text2Image

<details>
<summary>Create Model</summary>

```sql
CREATE MODEL aiforever
PREDICT url
USING
    engine = 'replicate',
    model_name= 'ai-forever/kandinsky-2',
    version ='2af375da21c5b824a84e1c459f45b69a117ec8649c2aa974112d7cf1840fc0ce',
    api_key = 'r8_BpO.........................';
```
</details>
<details>
<summary>Describe Model</summary>
You can use the `DESCRIBE PREDICTOR` query to see the available parameters that you can specify to customize your predictions:

```sql
DESCRIBE PREDICTOR mindsdb.aiforever.features;
```

### Output
```sql
+---------------------+-------------------+--------------------------------------------------------+---------+
| inputs              | default           | description                                            | type    |
+---------------------+-------------------+--------------------------------------------------------+---------+
| width               | 512               | Choose width. Lower the setting if out of memory.      | -       |
| height              | 512               | Choose height. Lower the setting if out of memory.     | -       |
| prompt              | red cat, 4k photo | Input Prompt                                           | string  |
| scheduler           | p_sampler         | Choose a scheduler                                     | -       |
| batch_size          | 1                 | Choose batch size. Lower the setting if out of memory. | -       |
| prior_steps         | 5                 | -                                                      | string  |
| guidance_scale      | 4                 | Scale for classifier-free guidance                     | number  |
| prior_cf_scale      | 4                 | -                                                      | integer |
| num_inference_steps | 50                | Number of denoising steps                              | integer |
+---------------------+-------------------+--------------------------------------------------------+---------+
```

</details>

<details>
<summary>Query Model for Image Generation</summary>

Now, you can use the established connection to query your ML Model as follows:

```sql
SELECT *
FROM aiforever
WHERE prompt='Great warrior Arjun from Mahabharata, looking at camera, cinematic lighting, 4k quality';
```

### OUTPUT
![GENERATE_IMAGE](./assets/Arjuna.png)


- IMPORTANT NOTE: PREDICTED **URL** will only work for **24 hours** after prediction.

> Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉

</details>


## Text2Video
<details>
<summary>Create Model</summary>

```sql
CREATE MODEL video_ai
PREDICT output
USING
    engine = 'replicate',
    model_name= 'deforum/deforum_stable_diffusion',
    version ='e22e77495f2fb83c34d5fae2ad8ab63c0a87b6b573b6208e1535b23b89ea66d6',
    api_key = 'r8_HEH............';
```
</details>
<details>
<summary>Describe Model</summary>
You can use the `DESCRIBE PREDICTOR` query to see the available parameters that you can specify to customize your predictions:

```sql
DESCRIBE PREDICTOR mindsdb.video_ai.features;
```

### Output 
```sql
+-------------------+---------+-----------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| inputs            | type    | default                                                               | description                                                                                                                                                          |
+-------------------+---------+-----------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| fps               | integer | 15                                                                    | Choose fps for the video.                                                                                                                                            |
| seed              | integer | -                                                                     | Random seed. Leave blank to randomize the seed                                                                                                                       |
| zoom              | string  | 0: (1.04)                                                             | zoom parameter for the motion                                                                                                                                        |
| angle             | string  | 0:(0)                                                                 | angle parameter for the motion                                                                                                                                       |
| sampler           | -       | plms                                                                  | -                                                                                                                                                                    |
| max_frames        | integer | 100                                                                   | Number of frames for animation                                                                                                                                       |
| translation_x     | string  | 0: (0)                                                                | translation_x parameter for the motion                                                                                                                               |
| translation_y     | string  | 0: (0)                                                                | translation_y parameter for the motion                                                                                                                               |
| color_coherence   | -       | Match Frame 0 LAB                                                     | -                                                                                                                                                                    |
| animation_prompts | string  | 0: a beautiful portrait of a woman by Artgerm, trending on Artstation | Prompt for animation. Provide 'frame number : prompt at this frame', separate different prompts with '|'. Make sure the frame number does not exceed the max_frames. |
+-------------------+---------+-----------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

</details>

<details>
<summary>Query Model for Video Generation</summary>

Now, you can use the established connection to query your ML Model as follows:

```sql
SELECT *
FROM video_ai
WHERE animation_prompts='a human and animals are friends by Asher Brown Durand, trending on Artstation'
USING
 max_frames=119;
```

### Output
```sql
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
| output                                                                                                                                                                                                                                                                                                                    | prompt                                 |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
| https://replicate.delivery/pbxt/gSgRjNlxIgJWBB8KeebKeRmBjZx0wqX7JC41U0pvIfPCYVzEB/out.mp4 | a human and animals are friends by Asher Brown Durand, trending on Artstation |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
```

You can watch the generated video [here](./assets/animals.mp4) as the provided link will

 be temporary and may not work.
This concludes the query for video generation using the MindsDB model. If you have any more questions or need further assistance, feel free to ask!
</details>

This is just few model used in above example there are more with vast variations and use cases. Also, there is no limit to imagination on how you can use this.

> **Note**: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
