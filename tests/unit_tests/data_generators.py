import random
import string
import datetime
from math import log


def generate_timeseries(length,
                        bounds=(0,1852255420),
                        _type='timestamp',
                        period=24*3600):
    column = []

    for n in range(*bounds, period):
        if len(column) >= length:
            break
        column.append(n)

    if _type == 'timestamp':
        return column
    elif _type == 'datetime':
        return list(map(str, map(datetime.datetime.fromtimestamp, column)))
    elif _type == 'date':
        return list(map(str, map(lambda x: datetime.datetime.fromtimestamp(x).date(), column)))


def rand_str(length=random.randrange(4,120)):
    # Create a list of unicode characters within the range 0000-D7FF
    random_unicodes = [chr(random.randrange(0xD7FF)) for _ in range(0, length)]
    return u"".join(random_unicodes)


def rand_ascii_str(length=30):
    charlist = [*string.ascii_letters]
    return ''.join(random.choice(charlist) for _ in range(length))


def rand_int():
    return int(random.randrange(-pow(2,18), pow(2,18)))


def rand_numerical_cat():
    return int(random.randrange(-pow(2,3), pow(2,3)))


def rand_float():
    return random.randrange(-pow(2,18), pow(2,18)) * random.random()


def generate_value_cols(types, length, ts_period=48*3600):
    columns = []
    for t in types:
        columns.append([])
        # This is a header of sorts
        columns[-1].append(rand_ascii_str(10))

        if t == 'int':
            gen_fun = rand_int
        elif t in ('date', 'datetime', 'timestamp'):
            columns[-1].extend(
                generate_timeseries(length=length, _type=t, period=ts_period))
            continue
        else:
            raise Exception(f'Unexpected type {t}')

        for n in range(length):
            val = gen_fun()
            # @TODO: Maybe escpae the separator rather than replace them
            if isinstance(val, str):
                val = val.replace(',', '_').replace('\n', '_').replace('\r','_')
            columns[-1].append(val)

    return columns


def generate_timeseries_labels(columns):
    labels = []
    # This is a header of sorts
    labels.append(rand_ascii_str(10))

    for n in range(1, len(columns[-1])):
        value = 1
        for i in range(len(columns)):
            if isinstance(columns[i][n], str):
                operand = len(columns[i][n])
            else:
                operand = columns[i][n]

            if i % 2 == 0:
                value = value * operand
            else:
                try:
                    value = value / operand
                except ValueError:
                    value = 1

        labels.append(value)

    return labels


def columns_to_file(columns, filename, headers=None):
    separator = ','
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
