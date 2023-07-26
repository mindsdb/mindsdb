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

CREATE MODEL aiforever
PREDICT url
USING
    engine = 'replicate',
    model_name= 'ai-forever/kandinsky-2',
    version ='2af375da21c5b824a84e1c459f45b69a117ec8649c2aa974112d7cf1840fc0ce',
    api_key = 'r8_BpO.........................';
```

You can use the DESCRIBE PREDICTOR query to see the available parameters that you can specify to customize your predictions:
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

Now, you can use the established connection to query your ML Model as follows:
```sql
SELECT *
FROM aiforever
WHERE prompt='Great warrior Arjun from Mahabharata, looking at camera,cinematic lighting, 4k quality';
```

### OUTPUT
![GENERATE_IMAGE](./assets/Arjuna.png)


- IMPORTANT NOTE: PREDICTED **URL** will only work for **24 hours** after prediction.

> Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
