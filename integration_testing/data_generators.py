import random
import string
import datetime
from math import log


def generate_timeseries(length, bounds=(0,1852255420), _type='timestamp',period=24*3600, swing=0, separator=','):
    column = []

    for n in range(*bounds,period):
        if len(column) >= length:
            break
        column.append(n)

    if _type == 'timestamp':
        return column
    elif _type == 'datetime':
        return list(map(str, map(datetime.datetime.fromtimestamp ,column)))
    elif _type == 'date':
        return list(map(str, map(lambda x: datetime.datetime.fromtimestamp(x).date() ,column)))


def rand_str(length=random.randrange(4,120)):
    # Create a list of unicode characters within the range 0000-D7FF
    # @TODO Copy pasted the 0xD7FF value, not 100% sure it returns all uncideo chars, maybe check that
    random_unicodes = [chr(random.randrange(0xD7FF)) for _ in range(0, length)]
    return u"".join(random_unicodes)


def rand_ascii_str(length=None, give_nulls=True, only_letters=False):
    if only_letters:
        charlist = [*string.ascii_letters]
    else:
        #other = [' ', '_', '-', '?', '.', '<', '>', ')', '(']
        other = []
        charlist = [*other, *string.ascii_letters]
    if length == None:
        length = random.randrange(1,120)
    if length % 4 == 0 and give_nulls==True:
        return ''
    #Sometimes we should return a number instead of a string
    #if length % 7 == 0:
    #    return str(length)
    return ''.join(random.choice(charlist) for _ in range(length))


def rand_int():
    return int(random.randrange(-pow(2,18), pow(2,18)))

def rand_numerical_cat():
    return int(random.randrange(-pow(2,3), pow(2,3)))


def rand_float():
    return random.randrange(-pow(2,18), pow(2,18)) * random.random()


def generate_value_cols(types, length, separator=',', ts_period=48*3600):
    columns = []
    for t in types:
        columns.append([])
        # This is a header of sorts
        columns[-1].append(rand_ascii_str(random.randrange(8,10),give_nulls=False,only_letters=True))

        # Figure out which random generation function to use for this column
        if t == 'str':
            gen_fun = rand_str
        elif t == 'ascii':
            gen_fun = rand_ascii_str
        elif t == 'int':
            gen_fun = rand_int
        elif t == 'nr_category':
            gen_fun = rand_numerical_cat
        elif t == 'float':
            gen_fun = rand_float
        else:
            columns[-1].extend(generate_timeseries(length=length,_type=t,period=ts_period, separator=separator))
            continue

        for n in range(length):
            val = gen_fun()
            # @TODO Maybe escpae the separator rather than replace
            if type(val) == str:
                val = val.replace(separator,'_').replace('\n','_').replace('\r','_')
            columns[-1].append(val)

    return columns


# Ignore all but flaots and ints
# Adds up the log of all floats and ints
def generate_labels_1(columns, separator=','):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(random.randrange(14,28),give_nulls=False,only_letters=True))

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
    labels.append(rand_ascii_str(random.randrange(5,11),give_nulls=False,only_letters=True))

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
                try:
                    value = value / operand
                except:
                    value = 1

        labels.append(value)

    return labels


def generate_labels_3(columns, separator=','):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(random.randrange(14,18),give_nulls=False,only_letters=True))

    col_nr = random.randrange(0,len(columns))
    labels.extend(columns[col_nr][1:])

    return labels


def columns_to_file(columns, filename, separator=',', headers=None):
    with open(filename, 'w', encoding='utf-8') as fp:
        fp.write('')

    with open(filename, 'a', encoding='utf-8') as fp:
        if headers is not None:
            fp.write(separator.join(headers) + '\r\n')
        for i in range(len(columns[-1])):
            row = ''
            for col in columns:
                row += str(col[i]) + separator

            fp.write(row.rstrip(separator) + '\r\n')
