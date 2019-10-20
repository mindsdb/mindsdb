from mindsdb import Predictor
import sys
import csv
from sklearn.metrics import accuracy_score, r2_score, confusion_matrix, roc_auc_score


def run(sample):
    if sample:
        train_file = 'train_sample.csv'
        test_file = 'test_sample.csv'
    else:
        print('Using full dataset')
        train_file = 'train.csv'
        test_file = 'test.csv'

    backend = 'lightwood'

    def get_real_test_data():
        test_reader = csv.reader(open(test_file, 'r'))
        next(test_reader, None)
        test_rows = [x for x in test_reader]
        return list(map(lambda x: int(x[-1]), test_rows))

    target_val_real = get_real_test_data()

    mdb = Predictor(name='default_on_credit_dp4')

    mdb.learn(to_predict='default.payment.next.month',from_data=train_file, stop_training_in_x_seconds=800,backend=backend, sample_margin_of_error=0.1, equal_accuracy_for_all_output_categories=True, unstable_parameters_dict={'optimize_model':True}, use_gpu=True)

    predictions = mdb.predict(when_data=test_file, unstable_parameters_dict={'always_use_model_prediction': True})

    cfz = 0
    cfo = 0
    lcfz = 0.00001
    lcfo = 0.00001
    for p in predictions:
        tv = str(p['default.payment.next.month'])
        if tv == '0':
            cfz += p['default.payment.next.month_confidence']
            lcfz += 1
        else:
            cfo += p['default.payment.next.month_confidence']
            lcfo += 1

    print('Confidence for 0: ')
    print(cfz/lcfz)

    print('Confidence for 1: ')
    print(cfo/lcfo)

    target_val_predictions = list(map(lambda x: x['default.payment.next.month'], predictions))

    for i in range(len(target_val_predictions)):
        try:
            target_val_predictions[i] = int(str(target_val_predictions[i]))
        except:
            target_val_predictions[i] = 2

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

    print(cm)
    print(f'Accuracy of {acc_0}% for predicting 0 labels')
    print(f'Accuracy of {acc_1}% for predicting 1 labels')
    return {
        'accuracy': auc
        ,'accuracy_function': 'roc_auc_score'
        ,'backend': backend
    }


# Run as main
if __name__ == '__main__':
    sample = bool(sys.argv[1]) if len(sys.argv) > 1 else False
    run(sample)
