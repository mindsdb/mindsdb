from pyod.models.ecod import ECOD  # unsupervised default
from pyod.models.xgbod import XGBOD  # semi-supervised default
from catboost import CatBoostClassifier  # supervised default
from pyod.utils.data import evaluate_print


def train_unsupervised(X_train):
    clf = ECOD()
    clf.fit(X_train)
    return clf


def train_semisupervised(X_train, y_train):
    clf = XGBOD(estimator_list=[ECOD()])
    clf.fit(X_train, y_train)
    return clf


def train_supervised(X_train, y_train):
    clf = CatBoostClassifier(logging_level="Silent")
    clf.fit(X_train, y_train)
    return clf


def get_predictions(clf, X, model_is_pyod):
    y_pred = clf.predict(X)
    if model_is_pyod:
        y_scores = clf.decision_function(X)
    else:
        y_scores = clf.predict_proba(X)[:, 1]
    return y_pred, y_scores


def print_results(classifier_name, y_train, y_test, y_train_scores, y_test_scores):
    print("\nOn Training Data:")
    evaluate_print(classifier_name, y_train, y_train_scores)
    print("\nOn Test Data:")
    evaluate_print(classifier_name, y_test, y_test_scores)
