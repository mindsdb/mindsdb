import sys
import subprocess


def install_dependencies(dependencies):
    outs = b''
    errs = b''
    result = {
        'success': False,
        'error_message': None
    }
    try:
        sp = subprocess.Popen(
            [sys.executable, '-m', 'pip', 'install', *dependencies],
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

    result['success'] = True
    return result
