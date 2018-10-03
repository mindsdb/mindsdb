def saveModel(model_object, mongo_collection, grid_fs_pointer):
    pass

def getColPermutations(possible_columns):
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

    ret = [perm.split(':') for perm in list(permutations.keys())]
    return ret



