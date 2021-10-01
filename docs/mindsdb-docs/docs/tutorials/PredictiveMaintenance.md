---
id: predictive-maintenance
title: Predictive Maintenance
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| High-Tech & Manufacturing | Operations | Data Scientist |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-RoboticFailure-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/others/robotic_failure/dataset)

This dataset contains force and torque measurements on a robot after failure detection. Each failure is characterized by 15 force/torque samples collected at regular time intervals.


{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/others/robotic_failure/dataset/test.csv', nrows=7) }}

<details>
  <summary>Click to expand Features Informations:</summary>

```
id
time
F_x
F_y
F_z
T_x
T_y
T_z
target

Fx1 ... Fx15 is the evolution of force Fx in the observation window
```
</details>

```python
import mindsdb_native
import pandas as pd
from sklearn.metrics import r2_score


def run():
    mdb = mindsdb_native.Predictor(name='robotic_failure')

    mdb.learn(from_data='dataset/train.csv', to_predict=['target'])

    predictions = mdb.predict(when='test.csv')

    pred_val = [x['target'] for x in predictions]
    real_val = list(pd.read_csv('dataset/test.csv')['target'])

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

## Mindsdb accuracy

| Accuracy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.8399922571492469 | Lightwood | 15 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.0-green)](https://pypi.org/project/MindsDB/1.16.0/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|


