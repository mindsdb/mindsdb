import pprint

pydantic_schema_description = """## Understanding Pydantic Schemas for JSON Formatting

Pydantic schemas provide a framework for defining the structure and validation rules for JSON output. Below is an overview of key components commonly found in a Pydantic schema:

### Key Components

Each object in the schema represents a Pydantic model in JSON format. Typical fields in a Pydantic model description include:

- **`anyOf`**:
  - A list describing possible values for a Pydantic model field.

- **`additionalProperties`**:
  - Describes the keys of a dictionary. Keys are always of type `string` due to this being a JSON Pydantic schema. The corresponding key types supported by Pydantic are:
    - `string`: a text string
    - `integer`: an integer number
    - `number`: a floating-point number

- **`items`**:
  - Describes the items contained within an `array` (list).

- **`type`**:
  - Specifies the Pydantic type assigned to the field, defining the expected data type. Common types include:
    - `string`: a text string
    - `integer`: an integer number
    - `number`: a floating-point number
    - `array`: a list
    - `object`: a dictionary
    - `null`: the python null value None. Indicates the field is optional.

- **`description`**:
  - Provides a textual narrative explaining the purpose and details of the output JSON field.

- **`title`**:
  - A Pydantic-generated, human-readable title for the field.

- **`default`**:
  - The default value for this field if no value is provided by the user.

### Schema

Below is the Pydantic schema:

{schema}

### Examples

Below is an example of well-formed output adhering to this schema.

- Dummy text strings are represented as "lorem ipsum."

{example}
"""


def get_dummy_value(field_value):
    """A function to return a dummy value of a Pydantic model field."""
    type_str = field_value["type"]
    example_dict = {
        "string": "lorem ipsum",
        "int": 3,
        "number": 42.0,
        "null": None,
        "object": {"lorem ipsum": "lorem_ipsum"},
    }

    if type_str in example_dict:
        return example_dict[type_str]
    else:
        return None


def get_dummy_array(field_value):
    """A function to return a dummy array of a Pydantic model field."""
    items = field_value["items"]

    if "type" in items:
        if items["type"] == "null":  # skip if null
            pass
        elif items["type"] == "array":  # is it an array?
            array_value = get_dummy_array(items)
        elif (
            items["type"] == "object" and "additionalProperties" in items
        ):  # is it a dict?
            array_value = get_dummy_dict(items)
        else:  # it is a regular value!
            array_value = get_dummy_value(items)
        return [array_value for _ in range(2)]

    elif "AnyOf" in field_value["items"]:
        array_value = get_any_of(field_value["items"])  # can be one of many types
        return [array_value for _ in range(2)]

    else:  # is it a pydantic class?
        array_value = example_generator(items)
        return [array_value for _ in range(2)]


def get_dummy_dict(field_value):
    """A function to return a dummy dictionary of a Pydantic model field."""
    return get_dummy_value(field_value)


def get_any_of(field_value):
    """A function to return the first viable pydantic type of an Any() Pydantic model field."""
    for any_of in field_value["anyOf"]:
        if "type" in any_of:
            if any_of["type"] == "null":  # skip if null
                continue
            elif any_of["type"] == "array":  # is it an array?
                out = get_dummy_array(any_of)
                return out
            elif (
                any_of["type"] == "object" and "additionalProperties" in any_of
            ):  # is it a dict?
                out = get_dummy_dict(any_of)
                return out
            else:  # it is a regular value!
                out = get_dummy_value(any_of)
                return out
        else:  # is it a pydantic class?
            out = example_generator(any_of)
            return out


def example_generator(pydantic_json_schema):
    """dynamically parse a pydantic object and generate an example of it's formatting."""

    example_dict = {}
    for schema_name, schema in pydantic_json_schema.items():

        for field_name, field_value in schema.items():
            if "type" in field_value:

                if field_value["type"] == "array":  # is it an array?
                    example_dict[field_name] = get_dummy_array(field_value)

                elif (
                    field_value["type"] == "object"
                    and "additionalProperties" in field_value
                ):  # is it a dict?
                    example_dict[field_name] = get_dummy_dict(field_value)

                else:  # it is a regular value!
                    example_dict[field_name] = get_dummy_value(field_value)

            elif "anyOf" in field_value:
                example_dict[field_name] = get_any_of(field_value)

            else:  # it is a pydantic class
                example_dict[field_name] = example_generator(field_value)
    return example_dict


def search_and_replace_refs(schema, defs, ref_skip={}, n=0):
    """Dynamically substitute subclass references in a Pydantic object schema."""
    for key, value in schema.items():
        if key in ref_skip:
            continue
        if type(value) is dict:
            if "$ref" in value:
                definition_key = value["$ref"].split("/")[-1]
                if definition_key in ref_skip:
                    schema[key] = {"type": "null"}
                else:
                    schema[key] = {definition_key: defs[definition_key]["properties"]}
            else:
                search_and_replace_refs(value, defs, ref_skip, n + 1)
        elif type(value) is list:
            for val in value:
                search_and_replace_refs(val, defs, ref_skip, n + 1)


def remove_extraneous_fields(schema, ref_skip):
    """Remove extraneous fields from object descriptions."""
    reduced_schema = schema["properties"]

    for ref in ref_skip.keys():
        if ref in reduced_schema:
            del reduced_schema[ref]

    for key, value in reduced_schema.items():
        if "title" in value:
            del value["title"]
        if "$defs" in value:
            del value["$defs"]
        if "required" in value:
            del value["required"]

    return reduced_schema


def format_for_prompt(pydantic_object, ref_skip={}):
    """Format a Pydantic object description for prompting an LLM."""
    schema = {k: v for k, v in pydantic_object.schema().items()}

    search_and_replace_refs(
        schema=schema["properties"], defs=schema["$defs"], ref_skip=ref_skip, n=0
    )

    reduced_schema = remove_extraneous_fields(schema, ref_skip)

    reduced_schema = {schema["title"]: reduced_schema}

    out = pprint.pformat(reduced_schema)

    return out, reduced_schema
