import pandas
import json

from mindsdb.libs.data_types.data_source import DataSource



class ArrayToColsDS(DataSource):

    def __init__(self, df, cols_to_split = {}):

        header = list(df.columns.values)
        data = df.values.tolist()

        new_data = []
        new_header = []

        for row in data:
            n_row = []
            for i, col in enumerate(header):
                cell = row[i]
                if col in cols_to_split:
                    ncols = cols_to_split[col]
                    if cell is None:
                        cells = [None]*ncols
                    else:
                        cells = json.loads(cell)

                    n_row += cells
                else:
                    n_row += [cell]

            new_data += [n_row]

        for col in header:

            if col in cols_to_split:
                ncols = cols_to_split[col]

                for i in range(ncols):
                    new_header += ['{col}_{num}'.format(col=col.replace('_agg',''), num=i)]
            else:
                new_header += [col]

        self.setDF(pandas.DataFrame(new_data, columns=new_header))

