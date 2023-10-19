# spaCy Handler

This is an integration with spaCy which is a free, open-source library for advanced Natural Language Processing (NLP) in Python

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
