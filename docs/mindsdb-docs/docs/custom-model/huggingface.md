# MindsDB and HuggingFace

HuggingFace facilitates building, training, and deploying ML models.

## How to Bring the HuggingFace Model to MindsDB

We use the `#!sql CREATE PREDICTOR` statement to bring the HuggingFace models to MindsDB.

Let's go through some sample models.

### Model 1: Spam Classifier

Here is an example of a binary classification. The model determines whether a text string is a spam or not.

```sql
CREATE MODEL mindsdb.spam_classifier
PREDICT PRED
USING
  engine = 'huggingface',
  task = 'text-classification',
  model_name = 'mrm8488/bert-tiny-finetuned-sms-spam-detection',
  input_column = 'text_spammy',
  labels = ['ham','spam'];
```

On execution, we get:

```sql
Query successfully completed
```

Before querying for predictions, we should verify the status of the `spam_classifier` model.

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'spam_classifier';
```

On execution, we get:

```sql
+---------------+-------+--------+--------+-------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|NAME           |PROJECT|STATUS  |ACCURACY|PREDICT|UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY|TRAINING_OPTIONS                                                                                                                                                                                               |
+---------------+-------+--------+--------+-------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|spam_classifier|mindsdb|complete|[NULL]  |PRED   |up_to_date   |22.10.2.1      |[NULL]|[NULL]           |{'target': 'PRED', 'using': {'engine': 'huggingface', 'task': 'text-classification', 'model_name': 'mrm8488/bert-tiny-finetuned-sms-spam-detection', 'input_column': 'text_spammy', 'labels': ['ham', 'spam']}}|
+---------------+-------+--------+--------+-------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Once the status is `complete`, we can query for predictions.

```sql
SELECT h.*, t.text_spammy AS input_text
FROM example_db.demo_data.hf_test AS t
JOIN mindsdb.spam_classifier AS h;
```

On execution, we get:

```sql
+----+---------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
|PRED|PRED_explain                                             |input_text                                                                                                                                                       |
+----+---------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
|spam|{'spam': 0.9051626920700073, 'ham': 0.09483727067708969} |Free entry in 2 a wkly comp to win FA Cup final tkts 21st May 2005. Text FA to 87121 to receive entry question(std txt rate)T&C's apply 08452810075over18's      |
|ham |{'ham': 0.9380123615264893, 'spam': 0.061987683176994324}|Nah I don't think he goes to usf, he lives around here though                                                                                                    |
|spam|{'spam': 0.9064534902572632, 'ham': 0.09354648739099503} |WINNER!! As a valued network customer you have been selected to receivea £900 prize reward! To claim call 09061701461. Claim code KL341. Valid 12 hours only.    |
+----+---------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Model 2: Sentiment Classifier

Here is an example of a multi-value classification. The model determines a sentiment of a text string, where possible values are negative (`neg`), neutral (`neu`), and positive (`pos`).

```sql
CREATE MODEL mindsdb.sentiment_classifier
PREDICT sentiment
USING
  engine = 'huggingface',
  task = 'text-classification',
  model_name = 'cardiffnlp/twitter-roberta-base-sentiment',
  input_column = 'text_short',
  labels = ['neg','neu','pos'];
```

On execution, we get:

```sql
Query successfully completed
```

Before querying for predictions, we should verify the status of the `sentiment_classifier` model.

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'sentiment_classifier';
```

On execution, we get:

```sql
+--------------------+-------+--------+--------+---------+-------------+---------------+------+-----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|NAME                |PROJECT|STATUS  |ACCURACY|PREDICT  |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY|TRAINING_OPTIONS                                                                                                                                                                                                    |
+--------------------+-------+--------+--------+---------+-------------+---------------+------+-----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|sentiment_classifier|mindsdb|complete|[NULL]  |sentiment|up_to_date   |22.10.2.1      |[NULL]|[NULL]           |{'target': 'sentiment', 'using': {'engine': 'huggingface', 'task': 'text-classification', 'model_name': 'cardiffnlp/twitter-roberta-base-sentiment', 'input_column': 'text_short', 'labels': ['neg', 'neu', 'pos']}}|
+--------------------+-------+--------+--------+---------+-------------+---------------+------+-----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Once the status is `complete`, we can query for predictions.

```sql
SELECT h.*, t.text_short AS input_text
FROM example_db.demo_data.hf_test AS t
JOIN mindsdb.sentiment_classifier AS h;
```

On execution, we get:

```sql
+---------+--------------------------------------------------------------------------------------+-------------------+
|sentiment|sentiment_explain                                                                     |input_text         |
+---------+--------------------------------------------------------------------------------------+-------------------+
|neg      |{'neg': 0.9732725620269775, 'neu': 0.022419845685362816, 'pos': 0.004307641182094812} |I hate Australia   |
|pos      |{'pos': 0.7607280015945435, 'neu': 0.2332666665315628, 'neg': 0.006005281116813421}   |I want to dance    |
|pos      |{'pos': 0.9835041761398315, 'neu': 0.014900505542755127, 'neg': 0.0015953202964738011}|Baking is the best |
+---------+--------------------------------------------------------------------------------------+-------------------+
```

### Model 3: Zero-Shot Classifier

Here is an example of a zero-shot classification. The model determines to which of the defined categories a text string belongs.

```sql
CREATE MODEL mindsdb.zero_shot_tcd
PREDICT topic
USING
  engine = 'huggingface',
  task = 'zero-shot-classification',
  model_name = 'facebook/bart-large-mnli',
  input_column = 'text_short',
  candidate_labels = ['travel', 'cooking', 'dancing'];
```

On execution, we get:

```sql
Query successfully completed
```

Before querying for predictions, we should verify the status of the `zero_shot_tcd` model.

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'zero_shot_tcd';
```

On execution, we get:

```sql
+-------------+-------+--------+--------+--------+-------------+---------------+------+-----------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|NAME         |PROJECT|STATUS  |ACCURACY|PREDICT |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY|TRAINING_OPTIONS                                                                                                                                                                                                         |
+-------------+-------+--------+--------+--------+-------------+---------------+------+-----------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|zero_shot_tcd|mindsdb|complete|[NULL]  |topic   |up_to_date   |22.10.2.1      |[NULL]|[NULL]           |{'target': 'topic', 'using': {'engine': 'huggingface', 'task': 'zero-shot-classification', 'model_name': 'facebook/bart-large-mnli', 'input_column': 'text_short', 'candidate_labels': ['travel', 'cooking', 'dancing']}}|
+-------------+-------+--------+--------+--------+-------------+---------------+------+-----------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Once the status is `complete`, we can query for predictions.

```sql
SELECT h.*, t.text_short AS input_text
FROM example_db.demo_data.hf_test AS t
JOIN mindsdb.zero_shot_tcd AS h;
```

On execution, we get:

```sql
+-------+--------------------------------------------------------------------------------------------------+-------------------+
|topic  |topic_explain                                                                                     |input_text         |
+-------+--------------------------------------------------------------------------------------------------+-------------------+
|travel |{'travel': 0.8343215584754944, 'dancing': 0.08680055290460587, 'cooking': 0.07887797057628632}    |I hate Australia   |
|dancing|{'dancing': 0.9746809601783752, 'travel': 0.015539299696683884, 'cooking': 0.009779711253941059}  |I want to dance    |
|cooking|{'cooking': 0.9936348795890808, 'travel': 0.0034196735359728336, 'dancing': 0.0029454431496560574}|Baking is the best |
+-------+--------------------------------------------------------------------------------------------------+-------------------+
```

### Model 4: Translation

Here is an example of a translation. The model gets an input string in English and translates it into French.

```sql
CREATE MODEL mindsdb.translator_en_fr
PREDICT translated
USING
  engine = 'huggingface',
  task = 'translation',
  model_name = 't5-base',
  input_column = 'text_short',
  lang_input = 'en',
  lang_output = 'fr';
```

On execution, we get:

```sql
Query successfully completed
```

Before querying for predictions, we should verify the status of the `translator_en_fr` model.

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'translator_en_fr';
```

On execution, we get:

```sql
+----------------+-------+--------+--------+----------+-------------+---------------+------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|NAME            |PROJECT|STATUS  |ACCURACY|PREDICT   |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY|TRAINING_OPTIONS                                                                                                                                                                   |
+----------------+-------+--------+--------+----------+-------------+---------------+------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|translator_en_fr|mindsdb|complete|[NULL]  |translated|up_to_date   |22.10.2.1      |[NULL]|[NULL]           |{'target': 'translated', 'using': {'engine': 'huggingface', 'task': 'translation', 'model_name': 't5-base', 'input_column': 'text_short', 'lang_input': 'en', 'lang_output': 'fr'}}|
+----------------+-------+--------+--------+----------+-------------+---------------+------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Once the status is `complete`, we can query for predictions.

```sql
SELECT h.*, t.text_short AS input_text
FROM example_db.demo_data.hf_test AS t
JOIN mindsdb.translator_en_fr AS h;
```

On execution, we get:

```sql
+-------------------------------+-------------------+
|translated                     |input_text         |
+-------------------------------+-------------------+
|Je déteste l'Australie         |I hate Australia   |
|Je veux danser                 |I want to dance    |
|La boulangerie est la meilleure|Baking is the best |
+-------------------------------+-------------------+
```

### Model 5: Summarisation

Here is an example of a summarisation.

```sql
CREATE MODEL mindsdb.summarizer_10_20
PREDICT text_summary
USING
  engine = 'huggingface',
  task = 'summarization',
  model_name = 'sshleifer/distilbart-cnn-12-6',
  input_column = 'text_long',
  min_output_length = 10,
  max_output_length = 20;
```

On execution, we get:

```sql
Query successfully completed
```

Before querying for predictions, we should verify the status of the `summarizer_10_20` model.

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'summarizer_10_20';
```

On execution, we get:

```sql
+----------------+-------+--------+--------+------------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|NAME            |PROJECT|STATUS  |ACCURACY|PREDICT     |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY|TRAINING_OPTIONS                                                                                                                                                                                                     |
+----------------+-------+--------+--------+------------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|summarizer_10_20|mindsdb|complete|[NULL]  |text_summary|up_to_date   |22.10.2.1      |[NULL]|[NULL]           |{'target': 'text_summary', 'using': {'engine': 'huggingface', 'task': 'summarization', 'model_name': 'sshleifer/distilbart-cnn-12-6', 'input_column': 'text_long', 'min_output_length': 10, 'max_output_length': 20}}|
+----------------+-------+--------+--------+------------+-------------+---------------+------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

Once the status is `complete`, we can query for predictions.

```sql
SELECT h.*, t.text_long AS input_text
FROM example_db.demo_data.hf_test AS t
JOIN mindsdb.summarizer_10_20 AS h;
```

On execution, we get:

```sql
+--------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|text_summary                                                                                                  |input_text                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
+--------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Australia is a sovereign country comprising the mainland of the Australian continent, the island of Tasmania  |Australia, officially the Commonwealth of Australia, is a sovereign country comprising the mainland of the Australian continent, the island of Tasmania, and numerous smaller islands.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|Dance is a performing art form consisting of sequences of movement, either improvised or purposefully selected|Dance is a performing art form consisting of sequences of movement, either improvised or purposefully selected. This movement has aesthetic and often symbolic value.[nb 1] Dance can be categorized and described by its choreography, by its repertoire of movements, or by its historical period or place of origin.                                                                                                                                                                                                                                                                                                                                                                                     |
|Baking is a method of preparing food that uses dry heat, typically in an oven                                 |Baking is a method of preparing food that uses dry heat, typically in an oven, but can also be done in hot ashes, or on hot stones. The most common baked item is bread but many other types of foods can be baked. Heat is gradually transferred from the surface of cakes, cookies, and pieces of bread to their center. As heat travels through, it transforms batters and doughs into baked goods and more with a firm dry crust and a softer center. Baking can be combined with grilling to produce a hybrid barbecue variant by using both methods simultaneously, or one after the other. Baking is related to barbecuing because the concept of the masonry oven is similar to that of a smoke pit.|
+--------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
