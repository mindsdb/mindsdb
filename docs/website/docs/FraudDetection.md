---
id: fraud-detection
title: Fraud Detection
---



```python
import mindsdb
import pandas as pd
from sklearn.metrics import balanced_accuracy_score

def run():
    backend='lightwood'

    mdb = mindsdb.Predictor(name='cc_fraud')

    mdb.learn(from_data='processed_data/train.csv', to_predict='Class', backend=backend, window_size=5)

    predictions = mdb.predict(when_data='processed_data/test.csv')

    pred_val = [int(x['Class']) for x in predictions]
    real_val = [int(x) for x in list(pd.read_csv(open('processed_data/test.csv', 'r'))['Class'])]

    accuracy = balanced_accuracy_score(real_val, pred_val)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]
      
    return {
        'accuracy': accuracy,
        'backend': backend,
        'additional info': additional_info
    }

# Run as main
if __name__ == '__main__':
    print(run())

```