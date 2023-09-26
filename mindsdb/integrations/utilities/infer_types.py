import pandas as pd


def infer_column_type(column, type=None):
    if not pd.api.types.is_object_dtype(column):
        return column

    if type is None or type == 'float':
        # Try to convert the entire column to integers
        try:
            return column.astype(float)
        except (ValueError, TypeError):
            pass

    if type is None or type == 'datetime':
        # If already a datetime type, leave it as such
        if pd.api.types.is_datetime64_any_dtype(column):
            return column

        # Check if the column can be converted to datetime
        try:
            return pd.to_datetime(column)
        except (pd.errors.ParserError, ValueError, TypeError):
            # If that fails, try to convert the entire column to floats
            pass

    if type is None or type == 'int':
        try:
            return column.astype(int)
        except (ValueError, TypeError):
            # If both fail, leave the column as a string
            return column.astype(str)

    return column


def infer_and_convert_column(df, column, sample_size=100, type=None):
    # Sample the column to analyze
    sample_data = df[column].sample(min(sample_size, len(df)))

    # Infer the type from the sample
    inferred_column = infer_column_type(sample_data, type=type)

    # Check if the inferred type is the same as the original type
    if inferred_column.dtype != sample_data.dtype:
        # If not, apply the inferred type to the entire column
        df[column] = infer_column_type(df[column], type=type)

    return df


def infer_and_convert_types(df):
    for column in df.columns:
        df = infer_and_convert_column(df, column)
