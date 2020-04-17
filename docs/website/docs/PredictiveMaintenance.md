---
id: predictive-maintenance
title: Predictive Maintenance
---

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
    return {
        'accuracy': accuracy
        , 'accuracy_function': 'balanced_accuracy_score'
        , 'backend': backend
    }


# Run as main
if __name__ == '__main__':
    print(run())
```