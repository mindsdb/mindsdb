---
id: customer-lifetime
title: Customer Lifetime Value Optimization
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Retail & Online | Marketing | Marketing Lead |


## Processed Dataset 


###### [![Data](https://img.shields.io/badge/GET--DATA-ImdbMovieReview-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/imdb_movie_review)

This is a [dataset](http://ai.stanford.edu/~amaas/data/sentiment/) for binary sentiment classification containing a set of 25,000 highly polar movie reviews for training, and 25,000 for testing. There is additional unlabeled data for use as well. Raw text and already processed bag of words formats are provided.

## Features informations
* review
* sentiment

## MindsDB Code example

```python
import mindsdb
import csv
import sys
from sklearn.metrics import accuracy_score


def run(sample):
    if sample:
        train_file = 'train_sample.tsv'
        test_file = 'test_sample.tsv'
    else:
        train_file = 'train.tsv'
        test_file = 'test.tsv'

    backend = 'lightwood'

    predictor = mindsdb.Predictor(name='imdb_predictor_x')

    predictor.learn(from_data=train_file, to_predict=['sentiment'], disable_optional_analysis=True, backend=backend,unstable_parameters_dict={'force_disable_cache': False},sample_margin_of_error=0.15)

    predictions = predictor.predict(when_data=test_file)

    predicted_sentiment = list(map(lambda x: x['sentiment'], predictions))

    real_sentiment = []

    with open(test_file, 'r') as raw_csv_fp:
        reader = csv.reader(raw_csv_fp, delimiter='\t')
        next(reader, None)
        for row in reader:
            real_sentiment.append(row[1])

    acc = accuracy_score(real_sentiment, predicted_sentiment) * 100
    print(f'Accuracy of {acc}% !')
    return {
        'accuracy': acc
        ,'accuracy_function': 'accuracy_score'
        ,'backend': backend
    }


# Run as main
if __name__ == '__main__':
    sample = bool(sys.argv[1]) if len(sys.argv) > 1 else False
    run(sample)
```

## Mindsdb accuracy


| Accuraccy       |  Backend  | Last run | MindsDB Version | Latest Version|
|----------------|--------------------|----------------------|-----------------|--------------|
| 0.8573 | Lightwood | 06 February 2020 | [![MindsDB](https://img.shields.io/badge/pypip--package-1.12.7-green)](https://pypi.org/project/MindsDB/1.12.7/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|