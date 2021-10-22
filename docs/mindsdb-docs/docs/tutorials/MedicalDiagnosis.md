---
id: medical-diagnosis
title: Medical Diagnosis
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Health Care | Health | Business Executive/Physician |

###### [![Data](https://img.shields.io/badge/GET--DATA-BreastCancer-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/classics/cancer50/processed_data)

Breast Cancer Wisconsin (Diagnostic) [Data Set](https://archive.ics.uci.edu/ml/datasets/Breast+Cancer+Wisconsin+%28Diagnostic%29). From the given information of the breast cancer dataset, classify whether it is a malignant cancer or benign cancer.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/cancer50/raw_data/cancer50.csv', nrows=7) }}

<details>
  <summary>Click to expand Features Information:</summary>

```
1. id ID number
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
```

</details>

## MindsDB Code example
```python
import mindsdb_native
import sys
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run():
    mdb = mindsdb_native.Predictor(name='cancer_model')

    mdb.learn(from_data='processed_data/train.csv', to_predict='diagnosis')

    test_df = pd.read_csv('processed_data/test.csv')
    predictions = mdb.predict(when_data='processed_data/test.csv')

    results = [str(x['diagnosis']) for x in predictions]
    real = list(map(str,list(test_df['diagnosis'])))

    accuracy = balanced_accuracy_score(real, results)

    return {
        'accuracy': accuracy
        ,'accuracy_function': 'balanced_accuracy_score'
    }

if __name__ == '__main__':
    result = run()
    print(result)
```

## Mindsdb accuracy


| Accuracy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.9666666666666667 | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|
