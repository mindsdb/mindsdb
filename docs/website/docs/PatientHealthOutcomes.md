---
id: patient-health
title: Patient Hearth Health  
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Health Care | Health | Business executive / Physician |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-HearthDisease-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/heart_disease/processed_data)

In the Heart Disease UCI dataset, the data comes from 4 databases: the Hungarian Institute of Cardiology, the University Hospital in Zurich, the University Hospital in Basel Switzerland, and the V.A. Medical Center Long Beach and Cleveland Clinic Foundation. The "goal" is to determine the presence of heart disease in the patient.

<details>
  <summary>Click to expand Features Informations:</summary>
1. age: age in years
2. sex: sex (1 = male; 0 = female)
3.  cp: chest pain type
    * Value 1: typical angina
    * Value 2: atypical angina
    * Value 3: non-anginal pain
    * Value 4: asymptomatic
4. trestbps: resting blood pressure (in mm Hg on admission to the hospital)
5. chol: serum cholestoral in mg/dl
6. fbs: (fasting blood sugar > 120 mg/dl) (1 = true; 0 = false)
7.  restecg: resting electrocardiographic results
    * Value 0: normal
    * Value 1: having ST-T wave abnormality (T wave inversions and/or ST elevation or depression of > 0.05 mV)
    * Value 2: showing probable or definite left ventricular hypertrophy by Estes' criteria
8. thalach: maximum heart rate achieved
9. exang: exercise induced angina (1 = yes; 0 = no)
10. oldpeak = ST depression induced by exercise relative to rest
11. slope: the slope of the peak exercise ST segment
    * Value 1: upsloping
    * Value 2: flat
    * Value 3: downsloping
12. ca: number of major vessels (0-3) colored by flourosopy
13. thal: 3 = normal; 6 = fixed defect; 7 = reversable defect
14. num: diagnosis of heart disease (angiographic disease status)
    * Value 0: < 50% diameter narrowing
    * Value 1: > 50% diameter narrowing
</details>

## MindsDB Code example
```python
import mindsdb
import pandas as pd
from sklearn.metrics import balanced_accuracy_score

def run():
    backend='lightwood'

    mdb = mindsdb.Predictor(name='hd')

    mdb.learn(from_data='processed_data/train.csv', to_predict='target', backend=backend, window_size=5)

    predictions = mdb.predict(when_data='processed_data/test.csv')

    pred_val = [int(x['target']) for x in predictions]
    real_val = [int(x) for x in list(pd.read_csv(open('processed_data/test.csv', 'r'))['target'])]

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
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.8256302521008403 | Lightwood | 16 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.0-green)](https://pypi.org/project/MindsDB/1.16.0/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|

<details>
  <summary>Click to expand MindsDB's explanation for each row:</summary>
```json
{
    'accuracy': 0.8256302521008403,
    'backend': 'lightwood',
    'additional info': [{
        'target': {
            'predicted_value': '1',
            'confidence': 0.621,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2355,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6432,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6219,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2631,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.651,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3055,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3015,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2431,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.295,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.64,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3095,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6754,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3077,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6697,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.3218,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6597,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6101,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6232,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.317,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2397,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3112,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6305,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2628,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6177,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3073,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6176,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2675,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6848,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.307,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2439,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3133,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2948,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2952,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2952,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6642,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2879,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2809,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2809,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2902,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6907,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6148,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2643,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.3059,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6952,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2412,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2743,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.286,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2594,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6489,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2741,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.264,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6625,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6646,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2914,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.3023,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.3083,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6808,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2993,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6579,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.6255,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2848,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2527,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2716,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2858,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.6673,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '1',
            'confidence': 0.2483,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.3078,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'target': {
            'predicted_value': '0',
            'confidence': 0.2874,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }]
}
```