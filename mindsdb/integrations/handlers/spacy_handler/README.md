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

##### RESULT

DF format
| review | recognition |
| ------ | ----------- |
| "Apple is looking at buying U.K. startup for $1 billion" | {('Apple', 'Number=Sing'), ('U.K.', 'Number=Sing'), ('for', ''), ('"', 'PunctSide=Fin\|PunctType=Quot'), ('$', ''), ('"', 'PunctSide=Ini|PunctType=Quot'), ('1', 'NumType=Card'), ('startup', 'Number=Sing'), ('at', ''), ('buying', 'Aspect=Prog|Tense=Pres|VerbForm=Part'), ('looking', 'Aspect=Prog|Tense=Pres|VerbForm=Part'), ('billion', 'NumType=Card'), ('is', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin')} |

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

### 1. With target column

```sql
SELECT input.review, output.recognition
FROM mysql_demo_db.amazon_reviews AS input
JOIN spacy__morphology__model AS output
LIMIT 1;
```

##### RESULT

DF Format
| review | recognition |
| ------ | ----------- |
| Late gift for my grandson. He is very happy with it. Easy for him (9yo ). | {('Late', 'Degree=Pos'), ('9yo', 'Number=Sing'), ('very', ''), ('gift', 'Number=Sing'), ('(', 'PunctSide=Ini\|PunctType=Brck'), ('it', 'Case=Acc|Gender=Neut|Number=Sing|Person=3|PronType=Prs'), ('with', ''), ('happy', 'Degree=Pos'), ('for', ''), ('him', 'Case=Acc|Gender=Masc|Number=Sing|Person=3|PronType=Prs'), ('.', 'PunctType=Peri'), (')', 'PunctSide=Fin|PunctType=Brck'), ('grandson', 'Number=Sing'), ('Easy', 'Degree=Pos'), ('my', 'Number=Sing|Person=1|Poss=Yes|PronType=Prs'), ('He', 'Case=Nom|Gender=Masc|Number=Sing|Person=3|PronType=Prs'), ('is', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin')} |
| I'm not super thrilled with the proprietary OS on this unit, but it does work okay and does what I need it to do. Appearance is very nice, price is very good and I can't complain too much - just wish it were easier (or at least more obvious) to port new apps onto it. For now, it helps me see things that are too small on my phone while I'm traveling. I'm a happy buyer. | {('the', 'Definite=Def\|PronType=Art'), ('now', ''), ('nice', 'Degree=Pos'), ('least', 'Degree=Sup'), ('or', 'ConjType=Cmp'), ('see', 'VerbForm=Inf'), ('new', 'Degree=Pos'), ('wish', 'VerbForm=Inf'), ('phone', 'Number=Sing'), ('this', 'Number=Sing|PronType=Dem'), ('-', ''), ('at', ''), ('work', 'VerbForm=Inf'), ('is', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin'), ('but', 'ConjType=Cmp'), ('very', ''), ('it', 'Case=Acc|Gender=Neut|Number=Sing|Person=3|PronType=Prs'), ('port', 'VerbForm=Inf'), ('not', 'Polarity=Neg'), ('to', ''), ('onto', ''), ('I', 'Case=Nom|Number=Sing|Person=1|PronType=Prs'), ('proprietary', 'Degree=Pos'), ('price', 'Number=Sing'), ('thrilled', 'Degree=Pos'), ("'m", 'Mood=Ind|Number=Sing|Person=1|Tense=Pres|VerbForm=Fin'), ('easier', 'Degree=Cmp'), ('For', ''), ('does', 'Mood=Ind|Number=Sing|Person=3|Tense=Pres|VerbForm=Fin'), ('too', ''), ('super', ''), ('me', 'Case=Acc|Number=Sing|Person=1|PronType=Prs'), ('that', 'PronType=Rel'), ('were', 'Mood=Ind|Tense=Past|VerbForm=Fin'), ('what', ''), ('with', ''), ('a', 'Definite=Ind|PronType=Art'), ('happy', 'Degree=Pos'), ('complain', 'VerbForm=Inf'), ('buyer', 'Number=Sing'), ("n't", 'Polarity=Neg'), ('and', 'ConjType=Cmp'), (')', 'PunctSide=Fin|PunctType=Brck'), ('it', 'Case=Nom|Gender=Neut|Number=Sing|Person=3|PronType=Prs'), ('small', 'Degree=Pos'), ('my', 'Number=Sing|Person=1|Poss=Yes|PronType=Prs'), ('do', 'VerbForm=Inf'), ('Appearance', 'Number=Sing'), ('more', 'Degree=Cmp'), ('are', 'Mood=Ind|Tense=Pres|VerbForm=Fin'), ('on', ''), ('need', 'Tense=Pres|VerbForm=Fin'), ('helps', 'Number=Sing|Person=3|Tense=Pres|VerbForm=Fin'), ('much', ''), ('just', ''), ('good', 'Degree=Pos'), ('OS', 'Number=Sing'), ('apps', 'Number=Plur'), ('traveling', 'Aspect=Prog|Tense=Pres|VerbForm=Part'), ('while', ''), ('.', 'PunctType=Peri'), ('okay', ''), ('obvious', 'Degree=Pos'), ('unit', 'Number=Sing'), (',', 'PunctType=Comm'), ('(', 'PunctSide=Ini|PunctType=Brck'), ('ca', 'VerbForm=Fin'), ('things', 'Number=Plur')} |

### 2. With attributes

```sql
CREATE MODEL model_spacy_ner
PREDICT recognition
USING
engine = 'spacy',
linguistic_feature = 'ner',
target_column = 'review';
```

```sql
SELECT input.review, output.label_, output.recognition, output.entity
FROM mysql_demo_db.amazon_reviews AS input
JOIN model_spacy_ner AS output
LIMIT 2;
```

##### RESULT

DF Format
| review | label\_ | recognition | entity |
| ------ | ------ | ----------- | ------ |
| Late gift for my grandson. He is very happy with it. Easy for him (9yo ). | ["ORDINAL"] | {(67, 70, 'ORDINAL')} | ["9yo"] |
| I'm not super thrilled with the proprietary OS on this unit, but it does work okay and does what I need it to do. Appearance is very nice, price is very good and I can't complain too much - just wish it were easier (or at least more obvious) to port new apps onto it. For now, it helps me see things that are too small on my phone while I'm traveling. I'm a happy buyer. | ["PERSON"] | {(114, 124, 'PERSON')} | ["Appearance"] |

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

### Attributes

- entity
- star_char
- end_char
- label\_

## Lemmatization

Lemmatization in linguistics is the process of grouping together the inflected forms of a word so they can be analysed as a single item, identified by the word's lemma, or dictionary form.

### Attributes

- lemma\_

## Dependency parsing

Dependency parsing is a linguistic analysis technique used in natural language processing to uncover grammatical relationships between words in a sentence.

### Attributes

- text
- dep\_
- head.text
- head.pos\_
- children

## Pos-tagging

spaCy can parse and tag a given Doc. This is where the trained pipeline and its statistical models come in, which enable spaCy to make predictions of which tag or label most likely applies in this context. A trained component includes binary data that is produced by showing a system enough examples for it to make predictions that generalize across the language – for example, a word following “the” in English is most likely a noun.

- text
- lemma\_
- pos\_
- tag\_
- dep\_
- shape\_
- is_alpha
- is_stop

## Morphology

Inflectional morphology is the process by which a root form of a word is modified by adding prefixes or suffixes that specify its grammatical function but do not change its part-of-speech. We say that a lemma (root form) is inflected (modified/combined) with one or more morphological features to create a surface form.

- token
- token.morph
