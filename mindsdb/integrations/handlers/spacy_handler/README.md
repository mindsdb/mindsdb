# spaCy Handler

This is an integration with spaCy which is a free, open-source library for advanced Natural Language Processing (NLP) in Python. The documentation is available here: https://spacy.io/usage

# Initialization

When initializing the model, it is important to define **"linguistic_feature"** and **"target_column"**. With lingustic feature, you define the type of NLP process you want to perform on specific column ("target_column").

In case of errors, for English model or any model you have to install it in the following way: **python -m spacy download en_core_web_sm**

```sql
CREATE MODEL spacy__morphology__model
PREDICT recognition
USING
engine = 'spacy',
linguistic_feature = 'morphology',
target_column = 'review';
```

After creating the model and defining the specifications, you are ready to perform NLP on columns.

## Perform NLP on single data

```sql
SELECT review, recognition
FROM spacy__morphology__model
WHERE review = '"Apple is looking at buying U.K. startup for $1 billion"';
```

RESULT:
{('is', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin'),
('startup', 'Number=Sing'), ('"', 'PunctSide=Ini|PunctType=Quot'),
('Apple', 'Number=Sing'), ('billion', 'NumType=Card'), ('at', ''),
('U.K.', 'Number=Sing'), ('buying', 'Aspect=Prog|Tense=Pres|VerbForm=Part'),
('1', 'NumType=Card'), ('looking', 'Aspect=Prog|Tense=Pres|VerbForm=Part'),
('for', ''),
('"', 'PunctSide=Fin|PunctType=Quot'),
('$', '')}

## Perform NLP on batch data

```sql
CREATE DATABASE mysql_demo_db
WITH ENGINE = "mysql",
PARAMETERS = {
    "user": "user",
    "password": "MindsDBUser123!",
    "host": "db-demo-data.cwoyhfn6bzs0.us-east-1.rds.amazonaws.com",
    "port": "3306",
    "database": "public"
    };
```

```sql
SELECT input.review, output.recognition
FROM mysql_demo_db.amazon_reviews AS input
JOIN spacy__morphology__model AS output
LIMIT 5;
```

# Linguistic Features

List of linguistic features and its values:

- Named Entity Recognition (NER): 'ner'
- Lemmatization: 'lemmatization'
- Dependency parsing: 'depedency-parsing'
- Pos-tagging: 'pos-tag'
- Morphology: 'morphology'

## Named Entity Recognition (NER)

The default trained pipelines can identify a variety of named and numeric entities, including companies, locations, organizations and products. You can add arbitrary classes to the entity recognition system, and update the model with new examples. (**the feature of training and fine-tuning the model will come later**)

A named entity is a “real-world object” that’s assigned a name – for example, a person, a country, a product or a book title. spaCy can recognize various types of named entities in a document, by asking the model for a prediction.

### Creating an ML Engine

The first step to make use of this handler is to create an ML Engine. This can be done using the following syntax,

```sql
CREATE MODEL spacy_ner_model
PREDICT recognition
USING
    engine = 'spacy';
```

### Using NER on random sentence

```sql
SELECT sentence, recognition
FROM spacy_ner_model
WHERE sentence = '"Apple is looking at buying U.K. startup for $1 billion"';
```

RESULT: {(28, 32, 'GPE'), (45, 55, 'MONEY'), (1, 6, 'ORG')}

The first and the second values are indices in the string that indicate the location
of the word in a sentence. The third value is a named entity.

## Lemmatization

Lemmatization in linguistics is the process of grouping together the inflected forms of a word so they can be analysed as a single item, identified by the word's lemma, or dictionary form.

## Dependency parsing

Dependency parsing is a linguistic analysis technique used in natural language processing to uncover grammatical relationships between words in a sentence.

## Pos-tagging

spaCy can parse and tag a given Doc. This is where the trained pipeline and its statistical models come in, which enable spaCy to make predictions of which tag or label most likely applies in this context. A trained component includes binary data that is produced by showing a system enough examples for it to make predictions that generalize across the language – for example, a word following “the” in English is most likely a noun.

## Morphology

Inflectional morphology is the process by which a root form of a word is modified by adding prefixes or suffixes that specify its grammatical function but do not change its part-of-speech. We say that a lemma (root form) is inflected (modified/combined) with one or more morphological features to create a surface form.
