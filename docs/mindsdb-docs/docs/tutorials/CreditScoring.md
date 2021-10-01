---
id: credit-scoring
title: Credit Scoring 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Financial Services | Finance | Business executive |


## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-DefaultofCreditCard-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/classics/german_credit_data/processed_data)

The German Credit dataset is a publically available from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/Statlog+%28German+Credit+Data%29). The dataset contains data on 20 variables and the classification whether an applicant is considered a Good or a Bad credit risks.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/german_credit_data/processed_data/test.csv', nrows=7) }}

<details>
  <summary>Click to expand Features Informations:</summary>

```
* Attribute 1: (qualitative)
    * Status of existing checking account
    *  A11 : ... < 0 DM
    *  A12 : 0 <= ... < 200 DM
    *  A13 : ... >= 200 DM / salary assignments for at least 1 year
    *  A14 : no checking account
* Attribute 2: (numerical)
    * Duration in month

* Attribute 3: (qualitative)
    * Credit history
    * A30 : no credits taken/ all credits paid back duly
    * A31 : all credits at this bank paid back duly
    * A32 : existing credits paid back duly till now
    * A33 : delay in paying off in the past
    * A34 : critical account/ other credits existing (not at this bank)

* Attribute 4: (qualitative)
    * Purpose
    * A40 : car (new)
    * A41 : car (used)
    * A42 : furniture/equipment
    * A43 : radio/television
    * A44 : domestic appliances
    * A45 : repairs
    * A46 : education
    * A47 : (vacation - does not exist?)
    * A48 : retraining
    * A49 : business
    * A410 : others

* Attribute 5: (numerical)
    * Credit amount

* Attibute 6: (qualitative)
    * Savings account/bonds
    * A61 : ... < 100 DM
    * A62 : 100 <= ... < 500 DM
    * A63 : 500 <= ... < 1000 DM
    * A64 : .. >= 1000 DM
    * A65 : unknown/ no savings account

* Attribute 7: (qualitative)
    * Present employment since
    * A71 : unemployed
    * A72 : ... < 1 year
    * A73 : 1 <= ... < 4 years
    * A74 : 4 <= ... < 7 years
    * A75 : .. >= 7 years

* Attribute 8: (numerical)
    * Installment rate in percentage of disposable income

* Attribute 9: (qualitative)
    * Personal status and sex
    * A91 : male : divorced/separated
    * A92 : female : divorced/separated/married
    * A93 : male : single
    * A94 : male : married/widowed
    * A95 : female : single

* Attribute 10: (qualitative)
    * Other debtors / guarantors
    * A101 : none
    * A102 : co-applicant
    * A103 : guarantor

* Attribute 11: (numerical)
    * Present residence since

* Attribute 12: (qualitative)
    * Property
    * A121 : real estate
    * A122 : if not A121 : building society savings agreement/ life insurance
    * A123 : if not A121/A122 : car or other, not in attribute 6
    * A124 : unknown / no property

* Attribute 13: (numerical)
    * Age in years

* Attribute 14: (qualitative)
    * Other installment plans
    * A141 : bank
    * A142 : stores
    * A143 : none

* Attribute 15: (qualitative)
    * Housing
    * A151 : rent
    * A152 : own
    * A153 : for free

* Attribute 16: (numerical)
    * Number of existing credits at this bank

* Attribute 17: (qualitative)
    * Job
    * A171 : unemployed/ unskilled - non-resident
    * A172 : unskilled - resident
    * A173 : skilled employee / official
    * A174 : management/ self-employed/
    * highly qualified employee/ officer

* Attribute 18: (numerical)
    * Number of people being liable to provide maintenance for

* Attribute 19: (qualitative)
    * Telephone
    * A191 : none
    * A192 : yes, registered under the customers name

* Attribute 20: (qualitative)
    * foreign worker
    * A201 : yes
    * A202 : no

```

</details>

## MindsDB Code example
```python
from mindsdb_native import Predictor
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run():

    mdb = Predictor(name='german_data')

    mdb.learn(to_predict='class', from_data='processed_data/train.csv')

    predictions = mdb.predict(when_data='processed_data/test.csv')

    predicted_val = [x.explanation['class']['predicted_value'] for x in predictions]
    real_val = list(pd.read_csv('processed_data/test.csv')['class'])

    accuracy = balanced_accuracy_score(real_val, predicted_val)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]

    return {
        'accuracy': accuracy,
        'accuracy_function': 'balanced_accuracy_score',
        'backend': backend,
        'single_row_predictions': additional_info
    }


# Run as main
if __name__ == '__main__':
    print(run())
```

## Mindsdb accuracy


| Accuracy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.64880952380 | Lightwood | 15 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.0-green)](https://pypi.org/project/MindsDB/1.16.0/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|
