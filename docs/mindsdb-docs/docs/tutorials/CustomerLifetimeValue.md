---
id: customer-lifetime
title: Customer Lifetime Value Optimization
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Retail & Online | Marketing | Marketing Lead |


## Processed Dataset


###### [![Data](https://img.shields.io/badge/GET--DATA-ImdbMovieReview-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/classics/imdb_movie_review)

This is a [dataset](http://ai.stanford.edu/~amaas/data/sentiment/) for binary sentiment classification containing a set of 25,000 highly popular movie reviews for training, and 25,000 for testing. There is additional unlabeled data for use as well. Raw text and already processed bag of words formats are provided.

## Features information
* review
* sentiment

## MindsDB Code example

```python
import mindsdb_native
from sklearn.metrics import accuracy_score


predictor = mindsdb_native.Predictor(name='movie_sentiment_predictor')
predictor.learn(from_data='train.tsv', to_predict=['sentiment'])

accuracy_data = predictor.test('test.tsv', accuracy_score)

accuracy_pct = accuracy_data['sentiment_accuracy'] * 100
print(f'Accuracy of {accuracy_pct}% !')
```

## Mindsdb accuracy


| Accuracy       |  Backend  | Last run | MindsDB Version | Latest Version|
|----------------|--------------------|----------------------|-----------------|--------------|
| 0.8573 | Lightwood | 06 February 2020 | [![MindsDB](https://img.shields.io/badge/pypip--package-1.12.7-green)](https://pypi.org/project/MindsDB/1.12.7/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|
