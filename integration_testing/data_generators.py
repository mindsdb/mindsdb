import random
import string
from math import log

def rand_str(length=random.randrange(4,120)):
    # Create a list of unicode characters within the range 0000-D7FF
    # @TODO Copy pasted the 0xD7FF value, not 100% sure it returns all uncideo chars, maybe check that
    random_unicodes = [chr(random.randrange(0xD7FF)) for _ in range(0, length)]
    return u"".join(random_unicodes)


def rand_ascii_str(length=random.randrange(4,120)):
    return ''.join(random.choice([*string.whitespace, *string.ascii_letters]) for _ in range(length))


def rand_int():
    return int(random.randrange(-pow(2,62), pow(2,62)))


def rand_float():
    return random.randrange(-pow(2,36), pow(2,36)) * random.random()


def generate_value_cols(types, length, separator=','):
    columns = []
    for t in types:
        columns.append([])
        # This is a header of sorts
        columns[-1].append(rand_ascii_str(random.randrange(5,20)).replace(separator,'ESCAPED_SEPARATOR').replace('\n','ESCAPED_SEPARATOR').replace('\r','ESCAPED_SEPARATOR'))

        # Figure out which random generation function to use for this column
        if t == 'str':
            gen_fun = rand_str
        elif t == 'ascii':
            gen_fun = rand_ascii_str
        elif t == 'int':
            gen_fun = rand_int
        elif t == 'float':
            gen_fun = rand_float
        else:
            gen_fun = rand_str

        for n in range(length):
            val = gen_fun()
            # @TODO Maybe escpae the separator rather than replace
            if type(val) == str:
                val = val.replace(separator,'ESCAPED_SEPARATOR').replace('\n','ESCAPED_SEPARATOR').replace('\r','ESCAPED_SEPARATOR')
                if '\n' in val or '\r' in val:
                    print(val)
                    exit()
            columns[-1].append(val)

    return columns


# Ignore all but flaots and ints
# Adds up the log of all floats and ints
def generate_labels_1(columns, separator=','):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(random.randrange(5,20)).replace(separator,'ESCAPED_SEPARATOR').replace('\n','ESCAPED_SEPARATOR').replace('\r','ESCAPED_SEPARATOR'))

    for n in range(1, len(columns[-1])):
        value = 0
        for i in range(len(columns)):
            try:
                value += log(abs(columns[i][n]))
            except:
                pass
        labels.append(value)

    return labels


def generate_labels_2(columns, separator=','):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(random.randrange(5,20)).replace(separator,'ESCAPED_SEPARATOR').replace('\n','ESCAPED_SEPARATOR').replace('\r','ESCAPED_SEPARATOR'))

    for n in range(1, len(columns[-1])):
        value = 1
        for i in range(len(columns)):
            if type(columns[i][n]) == str:
                operand = len(columns[i][n])
            else:
                operand = columns[i][n]

            if i % 2 == 0:
                value = value * operand
            else:
                value = value / operand

        labels.append(value)

    return labels


def generate_labels_3(columns, separator=','):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(random.randrange(5,20)).replace(separator,'ESCAPED_SEPARATOR').replace('\n','ESCAPED_SEPARATOR').replace('\r','ESCAPED_SEPARATOR'))

    col_nr = random.randrange(0,len(columns))
    labels.extend(columns[col_nr][1:])

    return labels


def columns_to_file(columns, filename, separator=','):
    with open(filename, 'w', encoding='utf-8') as fp:
        fp.write('')

    with open(filename, 'a', encoding='utf-8') as fp:
        print(columns[-1])
        for i in range(len(columns[-1])):
            row = ''
            for col in columns:
                row += str(col[i]) + separator

            fp.write(row.rstrip(separator) + '\n')
