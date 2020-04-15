---
id: fraud-detection
title: Fraud Detection
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Retail & Online | Finance | Business executive |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-DefaultofCreditCard-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/credit_card_fraud/processed_data)

The [datasets](https://www.kaggle.com/mlg-ulb/creditcardfraud) contains transactions made by credit cards in September 2013 by european cardholders.
This dataset presents transactions that occurred in two days, where we have 492 frauds out of 284,807 transactions. The dataset is highly unbalanced, the positive class (frauds) account for 0.172% of all transactions. The goal is to identify fraudulent credit card transactions. Feature 'Class' is the response variable and it takes value 1 in case of fraud and 0 otherwise.

<details>
  <summary>Click to expand Features Informations:</summary>
* Time Number of seconds elapsed between this transaction and the first transaction in the dataset
* V1may be result of a PCA Dimensionality reduction to protect user identities and sensitive features(v1-v28)
* V2
* V3
* V4
* V5
* V6
* V7
* V8
* V9
* V10
* V11
* V12
* V13
* V14
* V15
* V16
* V17
* V18
* V19
* V20
* V21
* V22
* V23
* V24
* V25
* V26
* V27
* V28abc
* AmountTransaction amount
* Class1 for fraudulent transactions, 0 otherwise

</details>

## MindsDB Code example

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

## Mindsdb accuracy

| Accuraccy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|--------------------|----------------------|-----------------|--------------|
| 0.64880952380 | Lightwood | 15 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.0-green)](https://pypi.org/project/MindsDB/1.16.0/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|


<details>
  <summary>Click to expand MindsDB's explanation for each row:</summary>

```json

```
</details>
