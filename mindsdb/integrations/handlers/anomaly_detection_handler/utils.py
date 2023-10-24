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
