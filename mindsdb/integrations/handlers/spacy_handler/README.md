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

### Example

#### Creating an ML Engine

The first step to make use of this handler is to create an ML Engine. This can be done using the following syntax,

```sql
CREATE MODEL spacy_ner_fast
PREDICT recognition
USING
engine = 'spacy',
linguistic_feature = 'ner',
target_column = 'review';
```

#### Using NER on random sentence

```sql
SELECT sentence, recognition
FROM spacy_ner_model
WHERE sentence = '"Apple is looking at buying U.K. startup for $1 billion"';
```

##### RESULT

DF Format
| review | recognition |
| ------ | ----------- |
| "Apple is looking at buying U.K. startup for $1 billion" | {(1, 6, 'ORG'), (45, 55, 'MONEY'), (28, 32, 'GPE')} |

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

### Example

#### Creating an ML Engine

```sql
CREATE MODEL model_pos_tag_spacy
PREDICT recognition
USING
engine = 'spacy',
linguistic_feature = 'pos-tag',
target_column = 'review';
```

#### Using pos-tag on batch data

```sql
SELECT input.review, output.tag_, output.pos_, output.shape_
FROM mysql_demo_db.amazon_reviews AS input
JOIN model_pos_tag_spacy AS output
LIMIT 3;
```

##### RESULT

DF Format
| review | tag* | pos* | shape\_ |
| ------ | ---- | ---- | ------ |
| Late gift for my grandson. He is very happy with it. Easy for him (9yo ). | ["JJ","NN","IN","PRP$","NN",".","PRP","VBZ","RB","JJ","IN","PRP",".","JJ","IN","PRP","-LRB-","NN","-RRB-","."] | ["ADJ","NOUN","ADP","PRON","NOUN","PUNCT","PRON","AUX","ADV","ADJ","ADP","PRON","PUNCT","ADJ","ADP","PRON","PUNCT","NOUN","PUNCT","PUNCT"] | ["Xxxx","xxxx","xxx","xx","xxxx",".","Xx","xx","xxxx","xxxx","xxxx","xx",".","Xxxx","xxx","xxx","(","dxx",")","."] |
| I'm not super thrilled with the proprietary OS on this unit, but it does work okay and does what I need it to do. Appearance is very nice, price is very good and I can't complain too much - just wish it were easier (or at least more obvious) to port new apps onto it. For now, it helps me see things that are too small on my phone while I'm traveling. I'm a happy buyer. | ["PRP","VBP","RB","RB","JJ","IN","DT","JJ","NN","IN","DT","NN",",","CC","PRP","VBZ","VB","RB","CC","VBZ","WP","PRP","VBP","PRP","TO","VB",".","NN","VBZ","RB","JJ",",","NN","VBZ","RB","JJ","CC","PRP","MD","RB","VB","RB","RB",":","RB","VB","PRP","VBD","JJR","-LRB-","CC","IN","RBS","RBR","JJ","-RRB-","TO","VB","JJ","NNS","IN","PRP",".","IN","RB",",","PRP","VBZ","PRP","VB","NNS","WDT","VBP","RB","JJ","IN","PRP$","NN","IN","PRP","VBP","VBG",".","PRP","VBP","DT","JJ","NN","."] | ["PRON","AUX","PART","ADV","ADJ","ADP","DET","ADJ","NOUN","ADP","DET","NOUN","PUNCT","CCONJ","PRON","AUX","VERB","ADV","CCONJ","VERB","PRON","PRON","VERB","PRON","PART","VERB","PUNCT","NOUN","AUX","ADV","ADJ","PUNCT","NOUN","AUX","ADV","ADJ","CCONJ","PRON","AUX","PART","VERB","ADV","ADV","PUNCT","ADV","VERB","PRON","AUX","ADJ","PUNCT","CCONJ","ADP","ADV","ADV","ADJ","PUNCT","PART","VERB","ADJ","NOUN","ADP","PRON","PUNCT","ADP","ADV","PUNCT","PRON","VERB","PRON","VERB","NOUN","PRON","AUX","ADV","ADJ","ADP","PRON","NOUN","SCONJ","PRON","AUX","VERB","PUNCT","PRON","AUX","DET","ADJ","NOUN","PUNCT"] | ["X","'x","xxx","xxxx","xxxx","xxxx","xxx","xxxx","XX","xx","xxxx","xxxx",",","xxx","xx","xxxx","xxxx","xxxx","xxx","xxxx","xxxx","X","xxxx","xx","xx","xx",".","Xxxxx","xx","xxxx","xxxx",",","xxxx","xx","xxxx","xxxx","xxx","X","xx","x'x","xxxx","xxx","xxxx","-","xxxx","xxxx","xx","xxxx","xxxx","(","xx","xx","xxxx","xxxx","xxxx",")","xx","xxxx","xxx","xxxx","xxxx","xx",".","Xxx","xxx",",","xx","xxxx","xx","xxx","xxxx","xxxx","xxx","xxx","xxxx","xx","xx","xxxx","xxxx","X","'x","xxxx",".","X","'x","x","xxxx","xxxx","."] |
| I purchased this Kindle Fire HD 8 was purchased for use by 5 and 8 yer old grandchildren. They basically use it to play Amazon games that you download. | ["PRP","VBD","DT","NNP","NNP","NNP","CD","VBD","VBN","IN","NN","IN","CD","CC","CD","NN","JJ","NNS",".","PRP","RB","VBP","PRP","TO","VB","NNP","NNS","IN","PRP","VBP","."] | ["PRON","VERB","DET","PROPN","PROPN","PROPN","NUM","AUX","VERB","ADP","NOUN","ADP","NUM","CCONJ","NUM","NOUN","ADJ","NOUN","PUNCT","PRON","ADV","VERB","PRON","PART","VERB","PROPN","NOUN","SCONJ","PRON","VERB","PUNCT"] | ["X","xxxx","xxxx","Xxxxx","Xxxx","XX","d","xxx","xxxx","xxx","xxx","xx","d","xxx","d","xxx","xxx","xxxx",".","Xxxx","xxxx","xxx","xx","xx","xxxx","Xxxxx","xxxx","xxxx","xxx","xxxx","."] |

### Attributes

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

### Attributes

- token
- token.morph
