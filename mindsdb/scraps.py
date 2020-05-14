'''
This file contains bits of codes that we might want to keep for later use,
, but that are are currently not used anywhere in the codebase.
'''

# flake8: noqa

from itertools import combinations, permutations


# Previously in: mindsdb/libs/helpers/train_helpers.py
def getAllButOnePermutations(possible_columns):

    permutations = {}

    for col in possible_columns:
        possible_columns_2 = [col3 for col3 in possible_columns if col3 != col ]
        n_perms = ":".join(possible_columns_2)

        permutations[n_perms] = 1

    ret = [perm.split(':') for perm in list(permutations.keys())]
    return ret


# Previously in mindsdb/libs/phases/stats_generator.py
# NOTE: This function used to confuse permutations & combinations,
# so I made both get_col_combinations() and get_col_permutations()
def get_col_combinations(columns, n=100):
    """
    Given a list of column names, finds first :param:n:
    combinations (without replacement).
    
    :param columns: list of column names
    :param n: max number of combinations
    
    :yields: example: [a, b, c] -> [[a], [b], [c], [a, b], [a, c], [b, c]]
    """

    count = 0
    for i in range(1, len(columns)):
        for combo in combinations(columns, i):
            if count < n:
                count += 1
                yield combo


def get_col_permutations(columns, n=100):
    """
    Given a list of column names, finds first :param:n: permutations
    
    :param columns: list of column names
    :param n: max number of permutations
    
    :yields: example: [a, b, c] -> [[a], [b], [c], [a, b], [b, a], [a, c], [c, a], [b, c], [c, b]]
    """

    count = 0
    for i in range(1, len(columns)):
        for perm in permutations(columns, i):
            if count < n:
                count += 1
                yield perm


def getBestFitDistribution(self, data, bins=40):
    """Model data by finding best fit distribution to data"""
    # Get histogram of original data

    y, x = np.histogram(data, bins=bins, density=False)
    x = (x + np.roll(x, -1))[:-1] / 2.0
    # Distributions to check
    DISTRIBUTIONS = [
        st.bernoulli, st.beta,  st.cauchy, st.expon,  st.gamma, st.halfcauchy, st.lognorm,
        st.norm, st.uniform, st.poisson
    ]

    # Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf
    # Estimate distribution parameters from data
    for i, distribution in enumerate(DISTRIBUTIONS):
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')
                # fit dist to data
                params = distribution.fit(data)
                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2]
                scale = params[-1]

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))
                # identify if this distribution is better
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution.name, best_params, x.tolist(), y.tolist())


def _get_params_as_dictionary(self, params):
    """ Returns a dictionary with the params of the distribution """
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]
    ret = {
        'loc': loc,
        'scale': scale,
        'shape': arg
    }
    return ret

# def isFullText(self, data):
#     """
#     It determines if the column is full text right
#     Right now we assume its full text if any cell contains any of the WORD_SEPARATORS
#
#     :param data: a list containing all the column
#     :return: Boolean
#     """
#     for cell in data:
#         try:
#             if any(separator in cell for separator in WORD_SEPARATORS):
#                 return True
#         except:
#             exc_type, exc_value, exc_traceback = sys.exc_info()
#             error = traceback.format_exception(exc_type, exc_value,
#                                       exc_traceback)
#             return False
#     return False

# Bool to number and vice-versa

boolean_dictionary = {True: 'True', False: 'False'}
numeric_dictionary = {True: 1, False: 0}
for column in df:
    if is_numeric_dtype(df[column]):
        df[column] = df[column].replace(numeric_dictionary)
    else:
        df[column] = df[column].replace(boolean_dictionary)
