---
id: churn-reduction
title: Customer Churn Reduction 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Telecomunications | Marketing | Marketing Lead |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-TelecomCustomerChurn-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/benchmarks/customer_churn/dataset)

Customer churn or customer turnover is the loss of clients or customers. Telecommunication companies often use customer attrition analysis and customer attrition rates as one of their key business metrics because the cost of retaining an existing customer is far less than acquiring a new one.  Use churn prediction models that predict customer churn by assessing their propensity of risk to churn.


<details>
  <summary>Click to expand Features Informations:</summary>

1. customerIDCustomer ID
2. genderWhether the customer is a male or a female
3. SeniorCitizenWhether the customer is a senior citizen or not (1, 0)
4. PartnerWhether the customer has a partner or not (Yes, No)
5. DependentsWhether the customer has dependents or not (Yes, No)
tenureNumber of months the customer has stayed with the company
6. PhoneServiceWhether the customer has a phone service or not (Yes, No)
7. MultipleLinesWhether the customer has multiple lines or not (Yes, No, No phone service)
8. InternetServiceCustomer’s internet service provider (DSL, Fiber optic, No)
9. OnlineSecurityWhether the customer has online security or not (Yes, No, No internet service)
10. OnlineBackupWhether the customer has online backup or not (Yes, No, No internet service)
11. DeviceProtectionWhether the customer has device protection or not (Yes, No, No internet service)
12. TechSupportWhether the customer has tech support or not (Yes, No, No internet service)
13. StreamingTVWhether the customer has streaming TV or not (Yes, No, No internet service)
14. StreamingMoviesWhether the customer has streaming movies or not (Yes, No, No internet service)
15. ContractThe contract term of the customer (Month-to-month, One year, Two year)
16. PaperlessBillingWhether the customer has paperless billing or not (Yes, No)
17. PaymentMethodThe customer’s payment method (Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic))
18. MonthlyChargesThe amount charged to the customer monthly
19. TotalChargesThe total amount charged to the customer
20 ChurnWhether the customer churned or not (Yes or No)
</details>

## MindsDB Code example
```python
import mindsdb
import pandas as pd
from sklearn.metrics import balanced_accuracy_score


def run():
    backend = 'lightwood'

    mdb = mindsdb.Predictor(name='employee_retention_model')

    mdb.learn(from_data='dataset/train.csv', to_predict='Churn', backend=backend,
              output_categories_importance_dictionary={'Yes': 1, 'No': 0.5},
              disable_optional_analysis=True)

    test_df = pd.read_csv('dataset/test.csv')
    predictions = mdb.predict(when_data='dataset/test.csv',
                              unstable_parameters_dict={'always_use_model_predictions': True})

    results = [str(x['Churn']) for x in predictions]
    real = list(map(str, list(test_df['Churn'])))

    accuracy = balanced_accuracy_score(real, results)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]
    return {
        'accuracy': accuracy,
        'accuracy_function': 'balanced_accuracy_score',
        'backend': backend,
        'additinal_indfo': additional_info
    }


if __name__ == '__main__':
    result = run()
    print(result)
```

## Mindsdb accuracy


| Accuraccy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.7659574468085106 | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|

<details>
  <summary>Click to expand MindsDB's explanation for test rows:
  </summary>
```json
{
    'accuracy': 0.7565675908943792,
    'accuracy_function': 'balanced_accuracy_score',
    'backend': 'lightwood',
    'additinal_indfo': [{
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3025,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3336,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3105,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3157,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6721,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3646,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3357,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2942,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3217,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2945,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2956,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3541,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2821,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.329,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3478,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3205,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3313,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3597,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3496,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2993,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3446,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3505,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7211,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7188,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2391,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3451,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3329,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3107,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6946,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3254,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3235,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3358,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6406,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3169,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3076,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3416,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3082,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2682,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6873,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2981,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3016,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3388,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3385,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7224,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3471,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2889,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7288,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3466,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3494,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2984,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3284,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6612,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3224,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3091,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3159,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3473,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3327,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2836,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2669,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6778,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3618,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3541,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3127,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3426,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3023,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3011,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7278,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3155,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3132,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3114,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3185,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3423,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2856,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2919,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6689,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3234,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3578,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3385,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3193,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7234,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3295,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3463,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3381,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.327,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2965,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6723,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6603,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3284,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3422,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3071,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.364,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3235,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2832,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3193,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3274,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.373,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3395,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3344,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2816,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3155,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2949,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3387,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3503,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3272,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3422,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3213,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3406,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3614,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6979,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3139,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6932,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3116,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2762,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2551,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3195,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7184,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3522,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6751,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3274,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3201,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3202,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3155,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3161,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3424,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3421,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2793,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.321,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3211,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3335,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2488,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7266,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6565,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3408,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3232,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3304,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.289,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2884,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3126,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6931,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3159,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3409,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3331,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3079,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3302,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.357,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3113,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2918,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3358,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6882,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3195,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3306,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6915,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3152,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3302,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3089,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3479,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3192,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6978,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3405,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2876,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3474,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2984,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3497,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3237,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3258,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3292,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2991,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2901,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2827,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3265,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3103,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2797,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3215,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3226,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3642,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6842,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7124,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3355,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3209,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3269,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3125,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7141,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3469,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6434,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3299,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.345,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3268,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3516,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6152,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3278,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6178,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3374,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2913,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3552,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3237,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2442,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3291,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2983,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6926,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7216,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3458,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.31,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3698,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2702,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.249,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3396,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3034,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3491,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3275,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.326,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6865,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3493,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.322,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3327,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2782,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3593,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3156,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3524,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3023,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.323,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3297,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3308,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3384,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7424,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7164,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3226,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3496,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6182,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2941,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3334,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3029,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.309,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7274,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3359,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3031,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3417,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3049,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7084,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3406,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.318,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3361,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3399,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3182,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2924,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2955,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3068,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3015,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3518,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3197,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3311,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.335,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2923,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3143,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7062,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3254,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.345,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3188,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3423,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.325,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.31,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3468,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7099,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2866,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3014,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3154,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3017,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2922,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.32,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.712,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3107,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3516,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3444,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3119,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3251,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6961,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6597,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3483,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3217,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2752,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3237,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.354,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3087,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3476,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3192,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3235,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3013,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3116,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.326,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3091,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.249,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3303,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3207,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6373,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2862,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3217,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3509,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3265,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2982,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3277,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3468,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3105,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2551,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3148,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7298,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3155,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3285,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3259,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3171,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3209,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3141,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3056,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3459,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7031,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3357,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3309,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6214,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3631,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3296,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3176,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2897,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3417,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2888,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3244,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3087,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.308,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3586,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2836,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2912,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3262,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3006,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2879,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3274,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6925,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.297,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3457,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.337,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2885,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3276,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3066,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3351,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7009,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3168,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3276,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6891,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.343,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2922,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3318,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2969,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6635,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3001,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7207,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.308,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3517,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3278,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2939,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3144,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6735,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7121,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7157,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3158,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3159,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.7129,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3037,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.2993,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3174,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.355,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3269,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.672,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3127,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2647,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3051,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3176,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3249,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3072,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.6732,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3282,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.358,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.7158,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3413,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2234,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3251,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3227,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3573,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3393,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.6907,
            'explanation': {
                'prediction_quality': 'confident',
                'important_missing_information': []
            },
            'prediction_quality': 'confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2887,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3063,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3421,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3197,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3648,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3007,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2854,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.3213,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.2984,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.3286,
            'explanation': {
                'prediction_quality': 'not very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'not very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.9264,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.9339,
            'explanation': {
                'prediction_quality': 'very confident',
                'important_missing_information': []
            },
            'prediction_quality': 'very confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'Yes',
            'confidence': 0.5589,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }, {
        'Churn': {
            'predicted_value': 'No',
            'confidence': 0.5514,
            'explanation': {
                'prediction_quality': 'somewhat confident',
                'important_missing_information': []
            },
            'prediction_quality': 'somewhat confident',
            'important_missing_information': []
        }
    }]
}
```
</details>