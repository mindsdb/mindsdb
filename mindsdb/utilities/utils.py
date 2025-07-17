import csv
import re
import typing


def parse_csv_attributes(csv_attributes: typing.Optional[str] = "") -> typing.Dict[str, str]:
    """
    Parse the raw_attributes variable, which uses the CSV format:
    key=value,another=something_else

    Returns:
        dict: Parsed key-value pairs as a dictionary.
    """
    attributes = {}

    if not csv_attributes:
        return attributes  # Return empty dictionary if the variable is not set

    try:
        # Use CSV reader to handle parsing the input
        reader = csv.reader([csv_attributes])
        for row in reader:
            for pair in row:
                # Match key=value pattern
                match = re.match(r'^\s*([^=]+?)\s*=\s*(.+?)\s*$', pair)
                if match:
                    key, value = match.groups()
                    attributes[key.strip()] = value.strip()
                else:
                    raise ValueError(f"Invalid attribute format: {pair}")
    except Exception as e:
        raise ValueError(f"Failed to parse csv_attributes='{csv_attributes}': {e}")

    return attributes
