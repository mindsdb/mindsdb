---
id: credit-scoring
title: Credit Scoring 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Financial Services | Finance | Business executive |


## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-DefaultofCreditCard-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/german_credit_data/processed_data)

The German Credit dataset is a publically available from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/Statlog+%28German+Credit+Data%29). The dataset contains data on 20 variables and the classification whether an applicant is considered a Good or a Bad credit risks.

<details>
  <summary>Click to expand Features Informations:</summary>
  
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


</details>

## MindsDB Code example
```python
from mindsdb import Predictor
import pandas as pd
from sklearn.metrics import balanced_accuracy_score, confusion_matrix


def run(sample=False):
    backend = 'lightwood'

    mdb = Predictor(name='german_data')

    mdb.learn(to_predict='class',from_data='processed_data/train.csv',backend=backend)

    predictions = mdb.predict(when_data='processed_data/test.csv', use_gpu=True)

    predicted_class = [x['class'] for x in predictions]
    real_class = list(pd.read_csv('processed_data/test.csv')['class'])

    accuracy = balanced_accuracy_score(real_class, predicted_class)
    print(f'Balacned accuracy score of {accuracy}')

    cm = confusion_matrix(real_class, predicted_class)

    print(cm)
    return {
        'accuracy': accuracy
        ,'accuracy_function': 'balanced_accuracy_score'
        ,'backend': backend
    }


# Run as main
if __name__ == '__main__':
    print(run())
```

## Mindsdb accuracy


| Accuraccy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.64880952380 | Lightwood | 15 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.0-green)](https://pypi.org/project/MindsDB/1.16.0/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|


<details>
  <summary>Click to expand MindsDB's explanation for each row:</summary>
```json
[{
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9046,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9033,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8825,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7153,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8968,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6981,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9125,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8293,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8666,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8953,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9105,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9063,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6584,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8987,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6782,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9038,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9046,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9034,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9043,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8439,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7535,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9043,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8877,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.5874,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9197,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8154,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8149,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9019,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8936,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7371,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.5657,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9042,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9194,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8974,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8684,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.809,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8493,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9029,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9036,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6824,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.904,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6617,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7116,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.589,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9019,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8118,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8558,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.863,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.904,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8859,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7422,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8905,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8716,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8945,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6931,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.5787,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8968,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.611,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.73,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8305,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9002,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8612,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8669,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9009,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7793,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8351,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9002,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6136,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7026,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.57,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8644,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8982,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.87,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7768,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8755,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9034,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8423,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9003,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7692,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7746,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8908,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8656,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.867,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9134,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8519,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6124,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8978,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9046,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9008,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9184,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8938,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8987,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9042,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6036,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.902,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7563,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8869,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8899,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8661,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8358,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8754,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9024,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8181,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9043,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8435,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9044,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8909,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9023,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.787,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9026,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8428,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8338,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9021,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8932,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9003,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9036,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7584,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9041,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9044,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9013,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.9147,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9028,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6759,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8972,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.717,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8758,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8846,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7568,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6393,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8951,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9046,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8863,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.5935,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8989,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.71,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.898,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8921,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8474,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8308,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6156,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8964,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.758,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8949,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8849,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.805,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8874,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9045,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.887,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8964,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8514,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9042,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.6647,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.881,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8984,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9033,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.909,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8051,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.8966,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9044,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9048,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8763,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8893,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9029,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.74,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6203,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7943,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9042,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9018,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.7216,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8743,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8879,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9004,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8964,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8901,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8802,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8076,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.904,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9021,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.6502,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8552,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.902,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9009,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8361,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8634,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9036,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9046,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.7336,
        'explanation': {
            'prediction_quality': 'confident',
            'important_missing_information': []
        },
        'prediction_quality': 'confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'bad',
        'confidence': 0.5688,
        'explanation': {
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        },
        'prediction_quality': 'somewhat confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9045,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9035,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9042,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.8864,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9044,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}, {
    'class': {
        'predicted_value': 'good',
        'confidence': 0.9047,
        'explanation': {
            'prediction_quality': 'very confident',
            'important_missing_information': []
        },
        'prediction_quality': 'very confident',
        'important_missing_information': []
    }
}]
```
</details>
