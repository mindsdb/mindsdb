# **Question Answering with OpenAI**

## **Introduction**

In this tutorial, we are going to demonstrate how to build a question answering model with MindsDB and OpenAI. The model's goal is to retrieve the answer to a question based on the given context. The [`natural_questions`]("https://huggingface.co/datasets/natural_questions/blob/main/dataset_infos.json) (**NQ**) corpus will be used as an input.

## **Data Setup**

To follow along, you need:

1- sign up for an account at [MindsDB](https://cloud.mindsdb.com/register/?_gl=1*1bhrxw8*_ga*MTI4MjkxNDc2NS4xNjc4OTQzMTY2*_ga_7LGFPGV6XV*MTY3OTA3MzQ1NS40LjEuMTY3OTA3NzM2OC42MC4wLjA.) or install it locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [pip](https://docs.mindsdb.com/setup/self-hosted/docker),

2- Download [MongoDB Compass](https://www.mongodb.com/try/download/compass) or [MongoDB Shell](https://www.mongodb.com/try/download/shell),

3- Download the [NQ]("https://huggingface.co/datasets/natural_questions/blob/main/dataset_infos.json) dataset,

4- Run the `MongoDB Compass` or `MongoDB Shell`

## **Connecting the Data**

To connect `MongoDB` with `MindsDB`:
- Open `MongoDB Compass` or `Mongo Shell`,
- Connect with URI: `mongodb://cloud.mindsdb.com`,
- If you are using the `MongoDB Shell`, you will be asked to type your `login-email` and `password`,
- If you are using `MongoDB Compass`, you need to add:
    - The `URI` in the URI box,
    - Click on `Advanced Connection Options` then select `Authentication`
    - Select `Username/Password` then type your credentials and hit `Connect`

> **Note**
> If you prefer the video tutorials, try the following:
> - [MongoDB Compass](https://docs.mindsdb.com/connect/mongo-compass)
> - [MongoDB Shell](https://docs.mindsdb.com/connect/mongo-shell)

After you connect `MongoDB` with `MindsDB`, you need to add the dataset using `inserOne` statement.

```
test> use mindsdb
mindsdb> db.databases.insertOne({
            'name': 'mongo_demo_db',
            'engine': 'mongodb',
            'connection_args': {
                "host": "mongodb+srv://<username>:<password>@<cluster host/ID>.mongodb.net/",
                "database": "public"
            }
        })
```

* `name`: Database name (name it as you wish)
*  `engine`: Here we use MongoDB so keep it as is
* `host`: connection string of your database in `MongoDB atlas`

The output:

```
{
  acknowledged: true,
  insertedId: ObjectId("641d997ed6cebaf05b92b909")
}
```

## **Understanding the Data**

We use the [NQ](https://huggingface.co/datasets/natural_questions/blob/main/dataset_infos.json) dataset which contain:
* id: a string feature.
* document a dictionary feature containing:
    * title: a string feature.
    * url: a string feature.
    * html: a string feature.
    * tokens: a dictionary feature containing:
        * token: a string feature.
        * is_html: a bool feature.
        * start_byte: a int64 feature.
        * end_byte: a int64 feature.
* question: a dictionary feature containing:
    * text: a string feature.
    * tokens: a list of string features.
* long_answer_candidates: a dictionary feature containing:
    * start_token: a int64 feature.
    * end_token: a int64 feature.
    * start_byte: a int64 feature.
    * end_byte: a int64 feature.
    * top_level: a bool feature.
* annotations: a dictionary feature containing:
    * id: a string feature.
    * long_answers: a dictionary feature containing:
      * start_token: a int64 feature.
        * end_token: a int64 feature.
        * start_byte: a int64 feature.
        * end_byte: a int64 feature.
        * candidate_index: a int64 feature.
* short_answers: a dictionary feature containing:
    * start_token: a int64 feature.
    * end_token: a int64 feature.
    * start_byte: a int64 feature.
    * end_byte: a int64 feature.
    * text: a string feature.
* yes_no_answer: a classification label, with possible values including NO (0), YES (1).

Let's have a look at the data we added earlier.

```

mindsdb> use mongo_demo_db
mongo_demo_db> db.naturalQuestions.findOne({})

```

The output:

```

{
  "id": "797803103760793766",
  "document": {
    "title": "Google",
    "url": "http://www.wikipedia.org/Google",
    "html": "<html><body><h1>Google Inc.</h1><p>Google was founded in 1998 By:<ul><li>Larry</li><li>Sergey</li></ul></p></body></html>",
    "tokens":[
      {"token": "<h1>", "start_byte": 12, "end_byte": 16, "is_html": True},
      {"token": "Google", "start_byte": 16, "end_byte": 22, "is_html": False},
      {"token": "inc", "start_byte": 23, "end_byte": 26, "is_html": False},
      {"token": ".", "start_byte": 26, "end_byte": 27, "is_html": False},
      {"token": "</h1>", "start_byte": 27, "end_byte": 32, "is_html": True},
      {"token": "<p>", "start_byte": 32, "end_byte": 35, "is_html": True},
      {"token": "Google", "start_byte": 35, "end_byte": 41, "is_html": False},
      {"token": "was", "start_byte": 42, "end_byte": 45, "is_html": False},
      {"token": "founded", "start_byte": 46, "end_byte": 53, "is_html": False},
      {"token": "in", "start_byte": 54, "end_byte": 56, "is_html": False},
      {"token": "1998", "start_byte": 57, "end_byte": 61, "is_html": False},
      {"token": "by", "start_byte": 62, "end_byte": 64, "is_html": False},
      {"token": ":", "start_byte": 64, "end_byte": 65, "is_html": False},
      {"token": "<ul>", "start_byte": 65, "end_byte": 69, "is_html": True},
      {"token": "<li>", "start_byte": 69, "end_byte": 73, "is_html": True},
      {"token": "Larry", "start_byte": 73, "end_byte": 78, "is_html": False},
      {"token": "</li>", "start_byte": 78, "end_byte": 83, "is_html": True},
      {"token": "<li>", "start_byte": 83, "end_byte": 87, "is_html": True},
      {"token": "Sergey", "start_byte": 87, "end_byte": 92, "is_html": False},
      {"token": "</li>", "start_byte": 92, "end_byte": 97, "is_html": True},
      {"token": "</ul>", "start_byte": 97, "end_byte": 102, "is_html": True},
      {"token": "</p>", "start_byte": 102, "end_byte": 106, "is_html": True}
    ],
  },
  "question" :{
    "text": "who founded google",
    "tokens": ["who", "founded", "google"]
  },
  "long_answer_candidates": [
    {"start_byte": 32, "end_byte": 106, "start_token": 5, "end_token": 22, "top_level": True},
    {"start_byte": 65, "end_byte": 102, "start_token": 13, "end_token": 21, "top_level": False},
    {"start_byte": 69, "end_byte": 83, "start_token": 14, "end_token": 17, "top_level": False},
    {"start_byte": 83, "end_byte": 92, "start_token": 17, "end_token": 20 , "top_level": False}
  ],
  "annotations": [{
    "id": "6782080525527814293",
    "long_answer": {"start_byte": 32, "end_byte": 106, "start_token": 5, "end_token": 22, "candidate_index": 0},
    "short_answers": [
      {"start_byte": 73, "end_byte": 78, "start_token": 15, "end_token": 16, "text": "Larry"},
      {"start_byte": 87, "end_byte": 92, "start_token": 18, "end_token": 19, "text": "Sergey"}
    ],
    "yes_no_answer": -1
  }]
}

```

## **Training a Predictor**

First of all, we need to create a question answering model using `openai` engine:

```

mongo_demo_db> use mindsdb
mindsdb> db.models.insertOne({
            name: 'question_answering',
            predict: 'answer',
            training_options: {
                        engine: 'openai',
                        prompt_template: 'answer the question of text:{{question_text}} about text:{{document_title}}'
            }
          })

```

The output:

```
{
  acknowledged: true,
  insertedId: ObjectId("641d997ed6cebaf05b92b8f8")
}

```

## **Status of a Predictor**

Next step is to check the status of a predictor. Once the above query has started execution, we can check the status of the creation process with the following query:

```

db.getCollection("models").find({"name": "question_answering"})

```

The output:

```

{
  NAME: 'question_answering',
  ENGINE: 'openai',
  PROJECT: 'mindsdb',
  VERSION: 1,
  STATUS: 'complete',
  ACCURACY: null,
  PREDICT: 'answer',
  UPDATE_STATUS: 'up_to_date',
  MINDSDB_VERSION: '23.3.4.0',
  ERROR: null,
  SELECT_DATA_QUERY: null,
  TRAINING_OPTIONS: "{'target': 'answer', 'using': {'prompt_template': 'answer the question of text:{{question_text}} about text:{{document_title}}'}}",
  CURRENT_TRAINING_PHASE: null,
  TOTAL_TRAINING_PHASES: null,
  TRAINING_PHASE_NAME: null,
  TAG: null,
  CREATED_AT: 2023-03-25T08:59:58.879Z,
  _id: ObjectId("000000000000008916041728")
}

```


## **Making Predictions**

### **Making a Single Prediction**

Once the creation is completed, the behavior is the same as with any other AI table. Here we are querying by specifying synthetic data in the actual query and the model we created i.e, `question_answering`:

```

> db.question_answering.find({
            question_text: "who founded google",
            document_title: "Google"
        })

```

The output:

```

{
  answer: 'Google was founded by Larry Page and Sergey Brin in 1998 while they were Ph.D. students at Stanford University.',
  question_text: 'who founded google',
  document_title: 'Google'
}

```

### **Making Batch Predictions**

We also can join model with the  collection for batch predictions:

```
mindsdb> db.question_answering.find(
            {
                'collection': 'mongo_demo_db.naturalQuestions'
            },
            {
                'question_answering.answer': 'answer',
                'naturalQuestions.question_text': 'question_text',
                'naturalQuestions.document_title': 'document_title'
            }
        ).limit(3)

```

The output:
```
{
  answer: "The main crops grown in the United States include corn, soybeans, wheat, cotton, fruits, and vegetables. Livestock such as cattle, pigs, and poultry are also important agricultural products in the United States. The United States is one of the largest agricultural producers in the world, and agriculture is a significant contributor to the country's economy.",
  question_text: 'what are the main crops grown in the united states',
  document_title: 'Agriculture in the United States'
}
{
  answer: 'The owner of Mandalay Bay in Las Vegas is MGM Resorts International.',
  question_text: 'who is the owner of the mandalay bay in vegas',
  document_title: 'Mandalay Bay'
}
{
  answer: "The Freedmen's Bureau had several functions, including providing food, medical care, and education to newly freed slaves. However, one of its functions was not to provide land ownership to former slaves.",
  question_text: 'which of the following was not one of the functions of the friedmans bureau',
  document_title: "Freedmen's Bureau"
}
```

The `naturalQuestions` collection is used to make the batch predictions. Upon joining the `question_answering` model we created. The model uses all values from the `document_title` and `question_text`