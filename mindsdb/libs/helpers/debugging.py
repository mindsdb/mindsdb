import os


def print_key_and_type(d, nl=''):
    for k in d:
        print(nl + str(k) + ' -- '+ str(type(d[k])))
        if isinstance(d[k], dict):
            print_key_and_type(d[k], nl + '  ')
        if isinstance(d[k], list):
            try:
                if isinstance(d[k][0], dict):
                    print('[\n')
                    print_key_and_type(d[k][0], nl + '  ')
                    print('\n]')
                else:
                    print(nl + '[' + str(type(d[k][0])) + ']')
            except:
                print('Empty list for key: ' + k)
