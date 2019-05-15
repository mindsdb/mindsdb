def getOneColPermutations(possible_columns):
    permutations = {col: 1 for col in possible_columns}
    ret = [perm.split(':') for perm in list(permutations.keys())]
    return ret
