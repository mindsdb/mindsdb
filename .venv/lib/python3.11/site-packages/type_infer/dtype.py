class dtype:
    """
    Definitions of all data types currently supported:

    - **Numerical**: Data that should be represented in the form of a number. Currently ``integer``, ``float``, and ``quantity`` are supported.
    - **Categorical**: Data that represents a class or label and is discrete. Currently ``binary``, ``categorical``, and ``tags`` are supported.
    - **Date/Time**: Time-series data that is temporal/sequential. Currently ``date``, and ``datetime`` are supported.
    - **Text**: Data that can be considered as language information.  Currently ``short_text``, and ``rich_text`` are supported. Short text has a small vocabulary (~ 100 words) and is generally a limited number of characters. Rich text is anything with greater complexity.
    - **Complex**: Data types that require custom techniques. Currently ``audio``, ``video`` and ``image`` are available, but highly experimental.
    - **Array**: Data in the form of a sequence where order must be preserved. ``tsarray`` dtypes are for "normal" columns that will be transformed to arrays at a row-level because they will be treated as time series.
    - **Miscellaneous**: Miscellaneous data descriptors include ``empty``, an explicitly unknown value versus ``invalid``, a data type not currently supported.
    
    Custom data types may be implemented here as a flag for subsequent treatment and processing. You are welcome to include your own definitions, so long as they do not override the existing type names (alternatively, if you do, please edit subsequent parts of the preprocessing pipeline to correctly indicate how you want to deal with these data types).
    """ # noqa

    # Numerical type data
    integer = "integer"
    float = "float"
    quantity = "quantity"

    # Categorical type data
    binary = "binary"
    categorical = "categorical"
    tags = "tags"

    # Dates and Times (time-series)
    date = "date"
    datetime = "datetime"

    # Text
    short_text = "short_text"
    rich_text = "rich_text"

    # Complex Data types
    image = "image"
    audio = "audio"
    video = "video"

    # Series/Sequences
    num_array = "num_array"
    cat_array = "cat_array"
    num_tsarray = 'num_tsarray'
    cat_tsarray = 'cat_tsarray'

    # Misc (Unk/NaNs)
    empty = "empty"
    invalid = "invalid"


# TODO: modifier class + system
