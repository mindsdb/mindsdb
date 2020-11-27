import os
import sys
import atexit
import tarfile
import subprocess
import shutil
import requests


assert os.name == 'posix'

def at_exit():
    os.system('echo Press Enter to exit ...;read foo')

def check_system_requirements():
    is_good = True
    required_utils = [("make", "brew install make"),
                      ("openssl", "brew install openssl")]

    for util, help_str in required_utils:
        res = subprocess.run(['which', util], stdout=subprocess.PIPE)
        if res.returncode:
            print("{} is required but not installed. Installation instruction: {}".format(util, help_str))
            is_good = False
    # we need brew only if not all requirements are met
    if not is_good:
        res = subprocess.run(['which', 'brew'], stdout=subprocess.PIPE)
        if res.returncode:
            print("brew is not installed. How to install: /usr/local/opt/openssl@1.1/bin/openssl")
        sys.exit("please install required utils and try again")

check_system_requirements()

atexit.register(at_exit)

# must be replaced in another script
NAME = '$name'
VERSION = '$version'

assert NAME != '$' + 'name'
assert VERSION != '$' + 'version'

PY_EMBED_URL = 'https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz'
GET_PIP_URL = 'https://bootstrap.pypa.io/get-pip.py'
VC_REDIST_URL = 'https://aka.ms/vs/16/release/vc_redist.x64.exe'

home_dir = os.getenv("HOME")

if len(sys.argv) < 2:
    INSTALL_DIR = os.path.join(home_dir, NAME)
else:
    INSTALL_DIR = os.path.join(os.path.abspath(sys.argv[1]), NAME)

PYTHON_SRC_DIR = PY_EMBED_URL.split('/')[-1][:-4]
PYTHON_SRC_DIR = os.path.join(INSTALL_DIR, PYTHON_SRC_DIR)
PYTHON_DIR = os.path.join(INSTALL_DIR, 'python')


def make_dir(d):
    if not os.path.isdir(d):
        os.makedirs(d)

def download_file(url):
    filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return filename

def get_openssl_path():
    res = subprocess.check_output(["which", "openssl"])
    res = res.decode('utf-8').rstrip()
    # delete /bin/openssl from path
    # from /usr/local/opt/openssl@1.1/bin/openssl -> /usr/local/opt/openssl@1.1
    root_dir = '/'.join(res.split('/')[:-2])
    return root_dir

try:
    make_dir(INSTALL_DIR)
except PermissionError as e:
    print('Please, run the installer as administrator (use "sudo")')
    os.system('pause')
    sys.exit(1)


print('extraction python source files')
py_embed_filename = download_file(PY_EMBED_URL)
with tarfile.open(py_embed_filename, 'r:gz') as z:
    z.extractall(INSTALL_DIR)
os.remove(py_embed_filename)

openssl_path = get_openssl_path()
os.environ["CPPFLAGS"] = "{} -I{}/include".format(os.getenv("CPPFLAGS", ""), openssl_path)
os.environ["LDFLAGS"] = "{} -L{}/lib".format(os.getenv("LDFLAGS", ""), openssl_path)
config_cmd = "cd {};./configure --prefix={} --with-openssl={} --with-ssl-default-suites=openssl --silent".format(PYTHON_SRC_DIR, PYTHON_DIR, openssl_path)


print("configuring python: {}".format(config_cmd))
print("this may take a while...")
print("CPPFLAGS: {}".format(os.getenv("CPPFLAGS", "")))
print("LDFLAGS: {}".format(os.getenv("LDFLAGS", "")))
status = os.system(config_cmd)
if status:
    sys.exit("python configuring process finished with error(s)")
print("installing python to %s" % PYTHON_DIR)
print("this may take a while...")

status = os.system("cd {}; make install --silent".format(PYTHON_SRC_DIR))
if status:
    sys.exit("installation process finished with error(s)")

shutil.rmtree(PYTHON_SRC_DIR)

PYTHON_EXEC = os.path.join(PYTHON_DIR, 'bin/python3')
print("python location: {}".format(PYTHON_EXEC))
os.system("{} -V".format(PYTHON_EXEC))

os.system('{} -m pip install torch==1.5.0 torchvision==0.6.0 -f https://download.pytorch.org/whl/torch_stable.html --no-warn-script-location'.format(PYTHON_EXEC))
if VERSION == '':
    os.system('{} -m pip install mindsdb --no-warn-script-location'.format(PYTHON_EXEC))
else:
    os.system('{} -m pip install mindsdb=={} --no-warn-script-location'.format(PYTHON_EXEC, VERSION))

print('generating run_server.sh')
with open(os.path.join(INSTALL_DIR, 'run_server.sh'), 'w') as f:
    lines = []
    if VERSION == '':
        lines.append('{} -m pip install mindsdb --upgrade --no-warn-script-location'.format(PYTHON_EXEC))
    lines.append('{} -m mindsdb'.format(PYTHON_EXEC))
    f.write('\n'.join(lines))
