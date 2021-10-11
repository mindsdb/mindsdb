---
id: churn-reduction
title: Customer Churn Reduction 
---

| Industry       | Department | Role               |
|----------------|------------|--------------------|
| Telecomunications | Marketing | Marketing Lead |

## Processed Dataset 

###### [![Data](https://img.shields.io/badge/GET--DATA-TelecomCustomerChurn-green)](https://github.com/mindsdb/mindsdb-examples/tree/master/classics/customer_churn/raw_data)

Customer churn or customer turnover is the loss of clients or customers. Telecommunication companies often use customer attrition analysis and customer attrition rates as one of their key business metrics because the cost of retaining an existing customer is far less than acquiring a new one.  Use churn prediction models that predict customer churn by assessing their propensity of risk to churn.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/churn/dataset/test.csv', nrows=7) }}


<details>
  <summary>Click to expand Features Information:</summary>

```
1. customerIDCustomer ID
2. gender Whether the customer is a male or a female
3. SeniorCitizen Whether the customer is a senior citizen or not (1, 0)
4. Partner Whether the customer has a partner or not (Yes, No)
5. Dependents Whether the customer has dependents or not (Yes, No)
tenureNumber of months the customer has stayed with the company
6. PhoneService Whether the customer has a phone service or not (Yes, No)
7. MultipleLines Whether the customer has multiple lines or not (Yes, No, No phone service)
8. InternetServiceCustomer’s internet service provider (DSL, Fiber optic, No)
9. OnlineSecurity Whether the customer has online security or not (Yes, No, No internet service)
10. OnlineBackup Whether the customer has online backup or not (Yes, No, No internet service)
11. DeviceProtection Whether the customer has device protection or not (Yes, No, No internet service)
12. TechSupport Whether the customer has tech support or not (Yes, No, No internet service)
13. StreamingTV Whether the customer has streaming TV or not (Yes, No, No internet service)
14. StreamingMovies Whether the customer has streaming movies or not (Yes, No, No internet service)
15. ContractThe contract term of the customer (Month-to-month, One year, Two year)
16. PaperlessBilling Whether the customer has paperless billing or not (Yes, No)
17. PaymentMethodThe customer’s payment method (Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic))
18. MonthlyChargesThe amount charged to the customer monthly
19. TotalChargesThe total amount charged to the customer
20 Churn Whether the customer churned or not (Yes or No)
```

</details>

## MindsDB Code example
```python
import mindsdb_native
import pandas as pd
from sklearn.metrics import accuracy_score


def run():

    mdb = mindsdb_native.Predictor(name='customer_churn_model')

    mdb.learn(from_data='dataset/train.csv', to_predict='Churn')

    test_df = pd.read_csv('dataset/test.csv')
    predictions = mdb.predict(when_data='dataset/test.csv')

    predicted_val = [x.explanation['Churn']['predicted_value'] for x in predictions]

    real_val = list(map(str, list(test_df['Churn'])))

    accuracy = accuracy_score(real_val, predicted_val)

    #show additional info for each transaction row
    additional_info = [x.explanation for x in predictions]

    return {
        'accuracy': accuracy,
        'accuracy_function': 'accuracy_score',
        'prediction_per_row': additional_info
    }


if __name__ == '__main__':
    result = run()
    print(result)
```

## Mindsdb accuracy


| Accuracy       | Backend  | Last run | MindsDB Version | Latest Version|
|----------------|-------------------|----------------------|-----------------|--------------|
| 0.7659574468085106 | Lightwood | 17 April 2020 | [![MindsDB](https://img.shields.io/badge/pypi--package-1.16.1-green)](https://pypi.org/project/MindsDB/1.16.1/)|   <a href="https://pypi.org/project/MindsDB/"><img src="https://badge.fury.io/py/MindsDB.svg" alt="PyPi Version"></a>|
