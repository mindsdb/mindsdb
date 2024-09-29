import os
import sys
import subprocess
from typing import Text, List


def install_dependencies(dependencies: List[Text]) -> dict:
    """
    Installs the dependencies for a handler by calling the `pip install` command via subprocess.

    Args:
        dependencies (List[Text]): List of dependencies for the handler.

    Returns:
        dict: A dictionary containing the success status and an error message if an error occurs.
    """
    outs = b''
    errs = b''
    result = {
        'success': False,
        'error_message': None
    }
    code = None

    try:
        # Split the dependencies by parsing the contents of the requirements.txt file.
        split_dependencies = parse_dependencies(dependencies)
    except FileNotFoundError as file_not_found_error:
        result['error_message'] = f"Error parsing dependencies, file not found: {str(file_not_found_error)}"
        return result
    except Exception as unknown_error:
        result['error_message'] = f"Unknown error parsing dependencies: {str(unknown_error)}"
        return result

    try:
        # Install the dependencies using the `pip install` command.
        sp = subprocess.Popen(
            [sys.executable, '-m', 'pip', 'install', *split_dependencies],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        code = sp.wait()
        outs, errs = sp.communicate(timeout=1)
    except subprocess.TimeoutExpired as timeout_error:
        sp.kill()
        result['error_message'] = f"Timeout error while installing dependencies: {str(timeout_error)}"
        return result
    except Exception as unknown_error:
        result['error_message'] = f"Unknown error while installing dependencies: {str(unknown_error)}"
        return result

    # Return the result of the installation if successful, otherwise return an error message.
    if code != 0:
        output = ''
        if isinstance(outs, bytes) and len(outs) > 0:
            output = output + 'Output: ' + outs.decode()
        if isinstance(errs, bytes) and len(errs) > 0:
            if len(output) > 0:
                output = output + '\n'
            output = output + 'Errors: ' + errs.decode()
        result['error_message'] = output
    else:
        result['success'] = True

    return result


def parse_dependencies(dependencies: List[Text]) -> List[Text]:
    """
    Recursively parses dependencies from a list of dependencies given in a requirements.txt file for a handler.
    This function will perform the following:
    1. Ignore standalone comments.
    2. Remove inline comments.
    3. Check if the dependency is a path to a requirements file and recursively parse the dependencies from that file.

    Args:
        dependencies (List[Text]): List of dependencies for a handler as read from the requirements.txt file.

    Returns:
        List[Text]: List of parsed dependencies for the handler.
    """
    # get the path to this script
    script_path = os.path.dirname(os.path.realpath(__file__))

    split_dependencies = []
    for dependency in dependencies:
        # ignore standalone comments
        if dependency.startswith('#'):
            continue

        # remove inline comments
        if '#' in dependency:
            dependency = dependency.split('#')[0].strip()

        # check if the dependency is a path to a requirements file
        if dependency.startswith('-r'):
            # get the path to the requirements file
            req_path = dependency.split(' ')[1]
            # create the absolute path to the requirements file
            abs_req_path = os.path.abspath(os.path.join(script_path, req_path.replace('mindsdb/integrations', '..')))
            # check if the file exists
            if os.path.exists(abs_req_path):
                inner_dependencies, inner_split_dependencies = [], []
                # read the dependencies from the file
                inner_dependencies = read_dependencies(abs_req_path)
                # recursively split the dependencies
                inner_split_dependencies = parse_dependencies(inner_dependencies)
                # add the inner dependencies to the split dependencies
                split_dependencies.extend(inner_split_dependencies)
            else:
                raise FileNotFoundError(f"Requirements file not found: {req_path}")

        else:
            split_dependencies.append(dependency)

    return split_dependencies


def read_dependencies(path: Text) -> List[Text]:
    """
    Reads the dependencies for a handler from the relevant requirements.txt file and returns them as a list.

    Args:
        path (Text): Path to the requirements.txt file for the handler.

    Returns:
        List[Text]: List of dependencies for the handler.
    """
    dependencies = []
    # read the dependencies from the file
    with open(str(path), 'rt') as f:
        dependencies = [x.strip(' \t\n') for x in f.readlines()]
        dependencies = [x for x in dependencies if len(x) > 0]
    return dependencies
