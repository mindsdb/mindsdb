'''
This file contains bits of codes that we might want to keep for later use,
, but that are are currently not used anywhere in the codebase.
'''

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
def getColPermutations(possible_columns, max_num_of_perms = 100):
    """
    Get all possible combinations given a list of column names
     :return: Given Input = [a,b,c]
             Then, Output=  [ [a], [b], [c], [a,b], [a,c], [b,c] ]
    """


     permutations = {col: 1 for col in possible_columns}

     for perm_size in range(len(possible_columns)-1):

         for permutation in list(permutations.keys()):

             tokens_in_perm = permutation.split(':')
            if len(tokens_in_perm) == perm_size:
                tokens_in_perm.sort()

                 for col in possible_columns:
                    if col in tokens_in_perm:
                        continue
                    new_perm = tokens_in_perm + [col]
                    new_perm.sort()
                    new_perm_string = ':'.join(new_perm)
                    permutations[new_perm_string] = 1

                     if len(permutations) > max_num_of_perms:
                        break

             if len(permutations) > max_num_of_perms:
                break

     ret = [perm.split(':') for perm in list(permutations.keys())]
    return ret




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
