---
id: predictive-maintenance
title: Predictive Maintenance
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| High-Tech & Manufacturing | Operations | Data Scientist |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-DefaultofCreditCard-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/german_credit_data/processed_data)


```python
import mindsdb
import pandas as pd
from sklearn.metrics import r2_score


def run():
    mdb = mindsdb.Predictor(name='robotic_failure')

    backend = 'lightwood'

    mdb.learn(from_data='dataset/train.csv', to_predict=['target'], order_by=['time'], window_size=14,
              group_by='id', disable_optional_analysis=True, backend=backend)

    predictions = mdb.predict(when='test.csv')

    pred_val = [x['target'] for x in predictions]
    real_val = list(pd.read_csv(open('dataset/test.csv', 'r'))['target'])

    accuracy = r2_score(real_val, pred_val)
    print(f'Got an r2 score of: {accuracy}')

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]

    return {
        'accuracy': accuracy,
         'accuracy_function': 'balanced_accuracy_score',
         'backend': backend,
         'additional_info': additional_info
    }


# Run as main
if __name__ == '__main__':
    print(run())
```

{'accuracy': 0.8399922571492469, 'accuracy_function': 'balanced_accuracy_score', 'backend': 'lightwood'}
