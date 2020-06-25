import os
import sys
import zipfile
import requests

assert os.name == 'nt'

PY_EMBED_URL = 'https://www.python.org/ftp/python/3.7.4/python-3.7.4-embed-amd64.zip'
GET_PIP_URL = 'https://bootstrap.pypa.io/get-pip.py'
VC_REDIST_URL = 'https://aka.ms/vs/16/release/vc_redist.x64.exe'

if len(sys.argv) < 2:
    sys.exit('Usage: ./{} install_dir'.format(__file__.split('.')[0]))


def make_dir(d):
    if not os.path.isdir(d):
        os.makedirs(d)


INSTALL_DIR = os.path.join(os.path.abspath(sys.argv[1]), 'mindsdb')
PYTHON_DIR = os.path.join(INSTALL_DIR, 'python')

make_dir(INSTALL_DIR)
make_dir(PYTHON_DIR)

PTH_PATH = os.path.join(PYTHON_DIR, 'python37._pth')


def download_file(url):
    filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return filename

print('installing vc_redist')
vc_redist_filename = download_file(VC_REDIST_URL)
os.system('{} /install /quiet /norestart'.format(vc_redist_filename))
os.remove(vc_redist_filename)

print('installing python')
py_embed_filename = download_file(PY_EMBED_URL)
with zipfile.ZipFile(py_embed_filename, 'r') as z:
    z.extractall(PYTHON_DIR)
os.remove(py_embed_filename)

print('fixing imports')

with open(PTH_PATH, 'a') as f:
    f.write('\nimport site')

with open(os.path.join(PYTHON_DIR, 'sitecustomize.py'), 'w') as f:
    f.write('import sys; sys.path.insert(0, "")')

PYTHON_EXE = os.path.join(PYTHON_DIR, 'python.exe')

get_pip_filename = download_file(GET_PIP_URL)
os.system('{} {} --no-warn-script-location'.format(PYTHON_EXE, get_pip_filename))
os.remove(get_pip_filename)

os.system('{} -m pip install torch==1.5.0+cpu torchvision==0.6.0+cpu -f https://download.pytorch.org/whl/torch_stable.html --no-warn-script-location'.format(PYTHON_EXE))
os.system('{} -m pip install mindsdb --no-warn-script-location'.format(PYTHON_EXE))

print('generating run_server.bat')
with open(os.path.join(INSTALL_DIR, 'run_server.bat'), 'w') as f:
    lines = [
        '{} -m pip install mindsdb --upgrade --no-warn-script-location'.format(PYTHON_EXE),
        '{} -m mindsdb'.format(PYTHON_EXE)
    ]
    f.write('\n'.join(lines))
