# Replicate Handler

This is the implementation of the Replicate handler for MindsDB.

## Replicate
Replicate is a platform and tool that aims to make it easier to work with machine learning models. It provides a library of open-source machine learning models that can be run in the cloud. Replicate allows users to deploy their own machine learning models at scale by providing infrastructure and automatic generation of API servers.


## Implementation
This handler was implemented using the `replicate` library that is provided by Replicate.

The required arguments to establish a connection are,

* model_name: Model name which you want to access in MindsDB. e.g 'air-forever/kandinsky-2'
* version: version hash/id which you want to use in MindsDB.
* api_key: API key from Replicate Platform you can found [here](https://replicate.com/account/api-tokens).


## Usage
To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).


To establish the connection and create a model in MindsDB, use the following syntax:
```sql

CREATE MODEL video_ai
PREDICT output
USING
    engine = 'replicate',
    model_name= 'deforum/deforum_stable_diffusion',
    version ='e22e77495f2fb83c34d5fae2ad8ab63c0a87b6b573b6208e1535b23b89ea66d6',
    api_key = 'r8_HEH............';
```

You can use the DESCRIBE PREDICTOR query to see the available parameters that you can specify to customize your predictions:
```sql
DESCRIBE PREDICTOR mindsdb.video_ai.features;
```
### OUTPUT 
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


Now, you can use the established connection to query your ML Model as follows:
```sql
SELECT *
FROM video_ai
WHERE animation_prompts='a human and animals are friends by Asher Brown Durand, trending on Artstation'
USING
 max_frames=119;
```

### OUTPUT
```sql
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
| output                                                                                                                                                                                                                                                                                                                    | prompt                                 |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
| https://replicate.delivery/pbxt/gSgRjNlxIgJWBB8KeebKeRmBjZx0wqX7JC41U0pvIfPCYVzEB/out.mp4 | a human and animals are friends by Asher Brown Durand, trending on Artstation |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------+
```

Checkout generated video [here](./assets/animals.mp4) as above link will not work, reason is given below.

- IMPORTANT NOTE: PREDICTED **URL** will only work for **24 hours** after prediction.

> Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
