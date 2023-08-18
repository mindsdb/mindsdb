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
CREATE MODEL blip
PREDICT text
USING
    engine = 'replicate',
    model_name= 'salesforce/blip',
    version ='2e1dddc8621f72155f24cf2e0adbde548458d3cab9f00c0139eea840d0ac4746',
    api_key = 'r8_BpO.........................';
```

You can use the DESCRIBE PREDICTOR query to see the available parameters that you can specify to customize your predictions:
```sql
DESCRIBE PREDICTOR mindsdb.blip.features;
```
### OUTPUT
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

## Visual Question Answering

Now, you can use the established connection to query your ML Model as follows:
```sql
SELECT *
FROM mindsdb.blip
WHERE image="https://images.unsplash.com/photo-1686847266385-a32745169de4"
AND question="Is there lion in image?"
USING 
task="visual_question_answering";
```
### OUTPUT
```sql
+------------+--------------------------------------------------------------+-------------------------+
| text        | image                                                        | question                |
+------------+--------------------------------------------------------------+-------------------------+
| Answer: no | https://images.unsplash.com/photo-1686847266385-a32745169de4 | Is there lion in image? |
+------------+--------------------------------------------------------------+-------------------------+
```

## Image Captioning 

```sql
SELECT *
FROM mindsdb.blip
WHERE image="https://images.unsplash.com/photo-1686847266385-a32745169de4"
```

### OUTPUT
```sql
+---------------------------------------------------+--------------------------------------------------------------+
| text                                               | image                                                        |
+---------------------------------------------------+--------------------------------------------------------------+
| Caption: a bird is sitting on the back of a horse | https://images.unsplash.com/photo-1686847266385-a32745169de4 |
+---------------------------------------------------+--------------------------------------------------------------+
```

Image Text Matching

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
| The image and text is matched with a probability of 0.7730.
The image feature and text feature has a cosine similarity of 0.3615. | https://images.unsplash.com/photo-1686847266385-a32745169de4 | Bird having horse Riding |
+-----------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------+--------------------------+
```
This is just an one model used in this example there are more with vast variation and usescases.
Also there is no limit to imagination, how can you use this.

 Note: Replicate provides only a few free predictions, so choose your predictions wisely. Don't let the machines have all the fun, save some for yourself! 😉
