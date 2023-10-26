from pyod.models.ecod import ECOD  # unsupervised default
from pyod.models.xgbod import XGBOD  # semi-supervised default
from catboost import CatBoostClassifier  # supervised default


def train_unsupervised(X_train, model=None):
    model = model if model is not None else ECOD()
    model.fit(X_train)
    return model


def train_semisupervised(X_train, y_train):
    model = XGBOD(estimator_list=[ECOD()])
    model.fit(X_train, y_train)
    return model


def train_supervised(X_train, y_train, model=None):
    model = model if model is not None else CatBoostClassifier(logging_level="Silent")
    model.fit(X_train, y_train)
    return model
