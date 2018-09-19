import pandas


from mindsdb.libs.data_types.data_source import DataSource




class WindowDS(DataSource):

    def __init__(self, df, col_max, col_min, window_size=300, step_size=30, min_size = 100):

        header = list(df.columns.values)
        data = df.values.tolist()

        max_index = header.index(col_max)
        min_index = header.index(col_min)

        ret = []

        for row in data:
            max = row[max_index]
            min = row[min_index]

            new_max = max
            new_min = max - window_size

            while new_max-min > min_size:

                row[max_index] = new_max
                row[min_index] = new_min

                ret += [row.copy()]

                new_max = new_max - step_size
                new_min = new_max - window_size if new_max - window_size > min else min

        self.setDF(pandas.DataFrame(ret, columns=header))
