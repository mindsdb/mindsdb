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
* model_type: It should be set to '**LLM**' while using Large language Model else it Optional


## Usage
To use this handler and connect to a Replicate cluster in MindsDB, you need an account on Replicate. Make sure to create an account by following this [link](https://replicate.com/signin?next=/account/api-tokens).


To establish the connection and create a model in MindsDB, use the following syntax:
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

You can use the DESCRIBE PREDICTOR query to see the available parameters that you can specify to customize your predictions:
```sql
DESCRIBE PREDICTOR mindsdb.vicuna_13b.features;
```

### OUTPUT
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

Now, you can use the established connection to query your ML Model as follows:
```sql
SELECT *
FROM vicuna_13b
WHERE prompt='Write a humourous  poem on Open Source'
USING
 max_length=200,
 temperature=0.75;
```
### OUTPUT
```sql
+--------------------------------------------------------------+----------------------------------------+
| output                                                       | prompt                                 |
+--------------------------------------------------------------+----------------------------------------+
| Opensource software, oh how we love thee                     | Write a humourous  poem on Open Source |  
| With bugs and glitches, oh so free                           |                                        |  
| You bring us laughter and joy each day                       |                                        |  
| And we'll never have to pay                                  |                                        |  
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




> Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
