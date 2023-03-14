## Classify and label text in your database with Hugging Face and MindsDB integration

Information is an incredibly valuable asset. 

Your databases are full of data … but extracting meaning from that text-based data is challenging. MindsDB, the [leading and fastest-growing open-source applied machine learning platform in the world](https://mindsdb.com/newsroom/mindsdb-now-the-leading-and-fastest-growing-applied-ml-platform-in-the-world-announces-over-70-data-integrations), makes integrating Natural Language Processing solutions into your databases seamless.

In this tutorial, you will learn how MindsDB integrates databases with pre-trained natural language models from Hugging Face, how to extract meaning from a sample database's text data, and how to convert that meaning into valuable insights with a sentiment analysis example. 

And the end of this tutorial, you will be able to deploy NLP models on your own data.

Let’s begin!

## The strategy, Transformers as AI Tables

Since the publication of "Attention is all you need" — a paper authored by Google researchers back in 2017 that introduced the "Transformer" deep neural network— the machine learning community has rapidly taken this neural architecture to new heights.

Nowadays, transformers are state-of-the-art in Natural Language Processing (the domain for which it was initially devised) and in other fields like image classification, image generation, 3D scene reconstruction, robotics, and protein folding.

Most business organizations that stand to benefit from Machine Learning (ML) don't have such exciting problems, however. Usually, data is stored in a structured format in some database or data warehouse. Transformers are well suited to this, too, enabling stunning performance in use cases like text classification, summarisation, and generation.

However, deploying models into production environments can be a formidable challenge, as data scientists and machine learning engineers tend to build pipelines with research-oriented tools, which means a costly transition, in both time and money, when a model is finally ready to be used live.

**One approach to simplify deployment for use cases with tabular data is considering machine learning models as tables in your database.** This strategy can significantly speed up model creation and deployment for developers and ML engineers. 

To achieve this, we can use MindsDB; an open-source applied ML platform that handles the engineering required to expose these models as tables from SQL. See an [“AI Tables” explained](https://www.youtube.com/watch?v=tnB4Y9T1E2k) video.

### What NLP tasks are possible inside databases? How it works.

All MindsDB integrations are defined by what are called "handlers." For ML frameworks, in particular, these Python classes explicitly define how an AI table should be created and called with SQL statements. Depending on the ML framework, this logic can be quite involved.

For Hugging Face's transformers, the handler exposes all pre-trained models in the Hugging Face Hub that support one of the following tasks:

*   **Standard text classification:** we have a set of possible predefined labels, and the model will classify some input text into one of them. Some of the most popular use cases, like sentiment analysis, hate speech or spam detection are usually framed as standard text classification tasks.
*   **Zero-shot text classification:** a variant of the standard formulation that provides labels dynamically without requiring any model retraining.
*   **Translation:** conceptually simple – pass some text in an original language and translate it into a target language.
*   **Summarization:** Summarize the input text without losing the original meaning.

Regarding input data, the expectation is to operate over a column containing text in any given table from your database. The output data type will vary depending on what use case is tackled.

Each use case will have slightly different procedures for data cleaning, which are internally defined as part of the handler logic.

Model artifacts are downloaded from the Hugging Face Hub and saved in common storage, which reduces time to create subsequent models. When such a model has been created, and it's time to generate predictions, the procedure feeds the input column into the transformer, produces an output column, and hands it over to the next stage, which will join the result according to the input SQL query.

The integration of Hugging Face with MindsDB unlocks the full potential of transformers across all supported data sources, in addition to interacting with other ML models that are also exposed as AI tables (and that may come from very different frameworks!).

### Concept recap before the tutorial

**NLP:** Natural language processing

**MindsDB:** Open-source software that brings Machine learning to Databases.

**Hugging Face:** Transformer library. Think about it as pre-trained machine learning models with specific tasks in mind.

**Sentiment Analysis:** The task of inferring emotions behind a text.

### Prerequisites

To follow along, you can sign up for an account at [**cloud.mindsdb.com**](https://cloud.mindsdb.com/register/huggingface). Alternatively, head to [**docs.mindsdb.com**](https://docs.mindsdb.com/) and follow the instructions to manually set up a local instance of MindsDB via docker or pip.

### Sentiment Analysis via SQL in practice

Let's see some action. This tutorial will go through a sequence of MindsDB-flavoured SQL commands that can get you a live transformer model to generate predictions for tables in your database. Note that for this example, you will use a table from our PostgreSQL public demo instance, so let’s start by connecting the MindsDB instance to it:  

```sql
CREATE DATABASE example_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    };
```

Now let’s check the demo data to be used in the example:

```sql
SELECT * FROM example_db.demo_data.user_comments;
```

Here is the output:

![](https://uploads-ssl.webflow.com/62e7d0b2c6d9597d811016ba/63936d2393ab7585692c83d1_the%20output.png)

Let’s mention one small nuance regarding switching between projects inside MindsDB. Projects are a natural way to keep artifacts (models, views, etc.) separate according to what predictive task they are solving, more about that [here](https://docs.mindsdb.com/). If you worked on another project or database previously, you can move to the default MindsDB project database called mindsdb by executing the following command: 

```sql
USE mindsdb;
```

Now let's enable a Hugging Face pre-trained classification-type model to identify sentiment for each of these sentences: 

```sql
CREATE MODEL sentiment_classifier
PREDICT sentiment
USING
  engine = 'huggingface',
  task = 'text-classification',
  model_name = "cardiffnlp/twitter-Roberta-base-sentiment",
  input_column = 'comment',
  labels = ['negative','neutral','positive'];
```

  
In practice, this points MindsDB to generate an “AI table” called sentiment\_classifier that will use the Hugging Face integration to predict a column named sentiment.

The USING clause specifies a few additional parameters that this particular “handler” requires. The task parameter defines the behaviour of the pre-trained model that is downloaded from the hub with the URL ending in model\_name. We declare the name of the input\_column and also optionally give our own names for the output labels of the model.

Please note that the input\_column key should match the column name from the demo table you have seen in the previous step. For example, we use the user\_comments table that has a comment column, as shown above. This is required for the JOIN queries to work (see next steps).

Once the above query has started execution, we can check the status of the creation process with the following query:

```sql
SELECT * FROM models
WHERE name='sentiment_classifier';
```

It may take a while to register as complete depending on the internet connection.

Once the creation is complete, the behavior is the same as with any other “AI table” – you can query it using SQL. Let’s specify some synthetic data first:

```sql
SELECT * FROM sentiment_classifier
WHERE comment ='It is really easy is to do NLP with MindsDB';
```
## Classify and label text in your database with Hugging Face and MindsDB integration

```sql
SELECT * FROM sentiment_classifier
WHERE comment ='It is really easy is to do NLP with MindsDB';
```

In our example, the model classified the sentiment correctly:

MindsDB also provides an ‘explain’ column with information about the probabilities of all sentiments, which is helpful for testing.

Finally, let’s apply sentiment analysis to the ‘user comments’ demo table. Use the command below to JOIN this table with the model, which will add the sentiment labels to the whole array.  

```sql
SELECT INPUT.comment, model.sentiment
FROM example_db.demo_data.user_comments AS INPUT
JOIN sentiment_classifier AS model;
```

![](https://uploads-ssl.webflow.com/62e7d0b2c6d9597d811016ba/63937f0bf0ae8e245bb5fafb_unnamed%20(4).png)

  
The above query returns the input text and its predicted class. At last, we can create predictive insights from our database using transformer models with minimal friction.

In this tutorial, you have learned how MindsDB integrates databases with pre-trained natural language models from Hugging Face, how to extract meaning from a sample database's text data, and how to convert that meaning into valuable insights with a sentiment analysis example. 

Now, you can deploy NLP models on your own data!

Get started with MindsDB at no cost today at [mindsdb.com](https://cloud.mindsdb.com/register/huggingface)   

Finally, if MindsDB's vision to democratize ML sounds exciting, head to our [community Slack](https://mindsdb.com/joincommunity), where you can get help, chat with others, and even get advice on how to build your own integration!