---
id: medical-diagnosis
title: Medical Diagnosis
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Health Care | Health | Business Executive/Physician |

###### [![Data](https://img.shields.io/badge/GET--DATA-BreastCancer-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/cancer50/processed_data)

Breast Cancer Wisconsin (Diagnostic) [Data Set](https://archive.ics.uci.edu/ml/datasets/Breast+Cancer+Wisconsin+%28Diagnostic%29). From the given information of the breast cancer dataset, classify whether it is a malignant cancer or benign cancer.

<details>
  <summary>Click to expand Features Informations:</summary>
1. idI D number
2. diagnosis The diagnosis of breast tissues (M = malignant, B = benign)
3. radius_mean mean of distances from center to points on the perimeter
4. texture_means tandard deviation of gray-scale values
5. perimeter_mean mean size of the core tumor
6. area_mean
7. smoothness_mean mean of local variation in radius lengths
8. compactness_mean mean of perimeter^2 / area - 1.0
9. concavity_mean mean of severity of concave portions of the contour
10. concave points_mean mean for number of concave portions of the contour
11. symmetry_mean
12. fractal_dimension_mean mean for "coastline approximation" - 1
13. radius_sestandard error for the mean of distances from center to points on the perimeter
14. texture_sestandard error for standard deviation of gray-scale values
15. perimeter_se
16. area_se
17. smoothness_sestandard error for local variation in radius lengths
18. compactness_sestandard error for perimeter^2 / area - 1.0
19. concavity_sestandard error for severity of concave portions of the contour
concave points_sestandard error for number of concave portions of the contour
20. symmetry_se
21. fractal_dimension_sestandard error for "coastline approximation" - 1
22. radius_worst"worst" or largest mean value for mean of distances from center to points on the perimeter
23. texture_worst"worst" or largest mean value for standard deviation of gray-scale values
24. perimeter_worst
25. area_worst
26. smoothness_worst "worst" or largest mean value for local variation in radius lengths
27. compactness_worst "worst" or largest mean value for perimeter^2 / area - 1.0
28. concavity_worst "worst" or largest mean value for severity of 29. 29. concave portions of the contour
30. concave points_worst "worst" or largest mean value for number of concave portions of the contour
31. symmetry_worst
32. fractal_dimension_worst"worst" or largest mean value for "coastline approximation" - 1
</details>

## MindsDB Code example
```python
import mindsdb
import sys
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run(sample):
    backend='lightwood'

    mdb = mindsdb.Predictor(name='cancer_model')

    mdb.learn(from_data='processed_data/train.csv', to_predict='diagnosis', backend=backend)

    test_df = pd.read_csv('processed_data/test.csv')
    predictions = mdb.predict(when_data='processed_data/test.csv', unstable_parameters_dict={'always_use_model_predictions': True})

    results = [str(x['diagnosis']) for x in predictions]
    real = list(map(str,list(test_df['diagnosis'])))

    accuracy = balanced_accuracy_score(real, results)

    return {
        'accuracy': accuracy
        ,'accuracy_function': 'balanced_accuracy_score'
        ,'backend': backend
    }

if __name__ == '__main__':
    sample = bool(sys.argv[1]) if len(sys.argv) > 1 else False
    result = run(sample)
    print(result)
```

## Mindsdb accuracy


| Accuraccy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.9666666666666667 | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|

<details>
  <summary>Click to expand MindsDB's explanation for test rows:
  </summary>

```json
{
    'accuracy': 0.9666666666666667,
    'accuracy_function': 'balanced_accuracy_score',
    'backend': 'lightwood',
    'additional_info': [{
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.775,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7335,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6401,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4522,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2977,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3101,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3506,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.318,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6602,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7002,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4436,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.2651,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3924,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6821,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7553,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.354,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3222,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.896,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6895,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.755,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8636,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3326,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6485,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3136,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3354,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6505,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.679,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7148,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6593,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6894,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7185,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7253,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6901,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4252,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3221,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7813,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.8737,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8305,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6851,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.701,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6864,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2707,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3537,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3199,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7504,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6575,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2759,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3535,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6745,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3157,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6887,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.661,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8222,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6957,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6465,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3309,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.2853,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7294,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7076,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6807,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3208,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
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
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7044,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3186,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7204,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6993,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7897,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7341,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3275,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7353,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7012,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6835,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3593,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7038,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3327,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8251,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7031,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3265,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4062,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8006,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3074,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7505,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6742,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7172,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3918,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6907,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3018,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7019,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3397,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3028,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3099,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3101,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3019,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7279,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3249,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6698,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6961,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3233,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.316,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3585,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3021,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6676,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3458,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6887,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6557,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3059,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3417,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7275,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.679,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7819,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3959,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6716,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.8572,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.775,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7335,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6401,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4522,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2977,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3101,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3506,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.318,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6602,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7002,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4436,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.2651,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3924,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6821,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7553,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.354,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3222,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.896,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6895,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.755,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8636,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3326,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6485,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3136,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3354,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6505,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.679,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7148,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6593,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6894,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7185,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7253,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6901,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4252,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3221,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7813,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.8737,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8305,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6851,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.701,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6864,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2707,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3537,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3199,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7504,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6575,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2759,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3535,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6745,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3157,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6887,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.661,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8222,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6957,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6465,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3309,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.2853,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7294,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7076,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6807,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3208,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
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
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7044,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3186,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7204,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6993,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7897,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7341,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3275,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7353,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7012,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6835,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3593,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7038,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3327,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8251,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7031,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3265,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4062,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8006,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3074,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7505,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6742,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7539,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4475,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7188,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3246,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7344,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.377,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3259,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3357,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3361,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3248,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7702,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3565,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6899,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7263,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3527,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3442,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4014,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3251,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6869,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3855,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.716,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6704,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3303,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3798,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7026,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8434,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4532,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6908,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.9491,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8338,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7765,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6488,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.531,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3173,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8263,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3361,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3921,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.347,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6767,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7319,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.5191,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.2738,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4484,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7069,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8066,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3968,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3528,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.9965,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7172,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.8077,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.9564,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3672,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6604,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3394,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3711,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6616,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7027,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7522,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
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
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.717,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7557,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7666,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.718,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4937,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3527,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8425,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.972,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.9105,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7095,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7315,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7128,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2799,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3947,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.348,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7998,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6729,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.2872,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3961,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7245,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6964,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3422,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.716,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6777,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8991,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7258,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6561,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3632,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3018,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7707,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7422,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7034,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3508,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3335,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7378,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3478,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7599,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7307,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8541,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7788,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3601,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7805,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7334,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7089,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.4041,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.737,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3674,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.9032,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.736,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3588,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4674,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8692,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3323,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.7539,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4475,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7188,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3246,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7344,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.377,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3259,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3357,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3361,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3248,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7702,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3565,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6899,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7263,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.3527,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3442,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4014,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3251,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6869,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3855,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.716,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.6704,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3303,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.3798,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7696,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.7026,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.8434,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.4532,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '0',
            'confidence': 0.6908,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'diagnosis': {
            'predicted_value': '1',
            'confidence': 0.9491,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }]
}
```
</details>