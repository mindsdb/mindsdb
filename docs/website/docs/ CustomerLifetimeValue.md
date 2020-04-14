---
id: customer-lifetime
title: Customer Lifetime Value Optimization
---



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