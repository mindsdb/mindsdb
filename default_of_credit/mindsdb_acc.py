from mindsdb import Predictor
import sys
import csv
from sklearn.metrics import accuracy_score, r2_score, confusion_matrix, roc_auc_score


def get_real_test_data():
    test_reader = csv.reader(open('test.csv', 'r'))
    next(test_reader, None)
    test_rows = [x for x in test_reader]
    return list(map(lambda x: int(x[-1]), test_rows))

target_val_real = get_real_test_data()

mdb = Predictor(name='default_on_credit_7')

mdb.learn(to_predict='default.payment.next.month',from_data="train.csv",backend='lightwood',unstable_parameters_dict={'balance_target_category':True})

predictions = mdb.predict(when_data='test.csv')
target_val_predictions = list(map(lambda x: x['default_payment_next_month'], predictions))

for i in range(len(target_val_predictions)):
    try:
        target_val_predictions[i] = float(target_val_predictions[i])
        if target_val_predictions[i] < 0.5:
            target_val_predictions[i] = 0
        else:
            target_val_predictions[i] = 1
    except:
        try:
            target_val_predictions[i] = int(target_val_predictions)
        except:
            target_val_predictions[i] = 0

nr_of_0 = len(list(filter(lambda x: x == 0, target_val_real)))
nr_of_1 = len(list(filter(lambda x: x == 1, target_val_real)))
print(f'The test dataset contains {nr_of_0} 0s and {nr_of_1} 1s')

acc = round(100*accuracy_score(target_val_real, target_val_predictions), 2)
print(f'Log loss accuracy of {acc}% !')

auc = roc_auc_score(target_val_real, target_val_predictions)
print(f'AUC score of {auc}')

cm = confusion_matrix(target_val_real, target_val_predictions)

acc_0 = round(100 * (cm[0][0]/nr_of_0),2)
acc_1 = round(100 * (cm[1][1]/nr_of_1),2)

print(f'Accuracy of {acc_0}% for predicting 0 labels')
print(f'Accuracy of {acc_1}% for predicting 1 labels')
