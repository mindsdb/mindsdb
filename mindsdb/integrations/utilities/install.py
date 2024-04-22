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
            # split the string into '-r' and the path
            r_flag, req_path = dependency.split(' ')
            # create the absolute path to the requirements file
            abs_req_path = os.path.abspath(os.path.join(script_path, req_path.replace('mindsdb/integrations', '..')))

            # check if the file exists
            if os.path.exists(abs_req_path):
                split_dependencies.append(r_flag)
                split_dependencies.append(abs_req_path)
            else:
                raise FileNotFoundError(f"Requirements file not found: {req_path}")

        else:
            split_dependencies.append(dependency)

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
