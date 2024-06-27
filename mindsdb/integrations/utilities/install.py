import os
import sys
import subprocess


def install_dependencies(dependencies):
    outs = b''
    errs = b''
    result = {
        'success': False,
        'error_message': None
    }

    split_dependencies = parse_dependencies(dependencies)

    try:
        sp = subprocess.Popen(
            [sys.executable, '-m', 'pip', 'install', *split_dependencies],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        code = sp.wait()
        outs, errs = sp.communicate(timeout=1)
    except Exception as e:
        result['error_message'] = str(e)

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

def parse_dependencies(dependencies):
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

def read_dependencies(path):
    dependencies = []
    # read the dependencies from the file
    with open(str(path), 'rt') as f:
        dependencies = [x.strip(' \t\n') for x in f.readlines()]
        dependencies = [x for x in dependencies if len(x) > 0]
    return dependencies