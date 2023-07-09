import re


def extract_collection_name(sql_query):
    # Regular expression pattern to match the collection name from the FROM clause
    pattern = r"FROM\s+\w+\.(\w+)"

    # Find the table name using regular expression
    match = re.search(pattern, sql_query, re.IGNORECASE)

    if match:
        table_name = match.group(1)
        return table_name
    else:
        return None
