import csv
import re
import typing

from pydantic import BaseModel, ValidationError


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
                match = re.match(r"^\s*([^=]+?)\s*=\s*(.+?)\s*$", pair)
                if match:
                    key, value = match.groups()
                    attributes[key.strip()] = value.strip()
                else:
                    raise ValueError(f"Invalid attribute format: {pair}")
    except Exception as e:
        raise ValueError(f"Failed to parse csv_attributes='{csv_attributes}': {e}") from e

    return attributes


def validate_pydantic_params(params: dict, schema: type[BaseModel], subject: str):
    # check names and types
    try:
        schema.model_validate(params)
    except ValidationError as e:
        problems = []
        for error in e.errors():
            parameter = ".".join([str(i) for i in error["loc"]])
            param_type = error["type"]
            if param_type == "extra_forbidden":
                msg = f"Parameter '{parameter}' is not allowed"
            else:
                msg = f"Error in '{parameter}' (type: {param_type}): {error['msg']}. Input: {repr(error['input'])}"
            problems.append(msg)

        msg = "\n".join(problems)
        if len(problems) > 1:
            msg = "\n" + msg
        raise ValueError(f"Problem with {subject} parameters: {msg}") from e
