import numpy as np
from sklearn.metrics import r2_score


def evaluate_regression_accuracy(
        y_true,
        y_pred,
):
    """
    Evaluates accuracy for regression tasks.
    If predictions have a lower and upper bound, then `within-bound` accuracy is computed: whether the ground truth value falls within the predicted region.
    If not, then a (positive bounded) R2 score is returned instead.

    :return: accuracy score as defined above. 
    """  # noqa
    if 'lower' and 'upper' in y_pred:
        y = np.array(y_true).astype(float)
        within = ((y >= y_pred['lower']) & (y <= y_pred['upper']))
        return sum(within) / len(within)
    else:
        r2 = r2_score(y_true, y_pred['prediction'])
        return max(r2, 0)
