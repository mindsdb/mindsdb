import os
import sys
import atexit
import tarfile
import subprocess
import shutil
import argparse

import requests

REQUIRED_UTILS = {"make": "brew install make",
                  "openssl": "brew install openssl",
                  "zlib": "brew install zlib",
                  "sqlite": "brew install sqlite",
                  "bzip2": "brew install bzip2",
                  "libiconv": "brew install libiconv",
                  "libzip": "brew install libzip"}

def at_exit():
    os.system('echo Press Enter to exit ...;read foo')

def check_system_requirements():
    res = subprocess.run(['which', 'brew'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode:
        print("brew is not installed. How to install: /usr/local/opt/openssl@1.1/bin/openssl")
        sys.exit("please install required utils and try again")
    is_good = True

    for util in REQUIRED_UTILS:
        res = subprocess.run(['brew', "--prefix", util], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if res.returncode:
            print("{} is required but not installed. Installation instruction: {}".format(util, REQUIRED_UTILS[util]))
            is_good = False
    if not is_good:
        sys.exit("please install required utils and try again")


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

def get_util_dir(util):
    res = subprocess.check_output(["brew", "--prefix", util])
    return res.decode('utf-8').rstrip()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MindsDB Installer')
    parser.add_argument("--install_dir", type=str, default=os.getenv("HOME"))
    parser.add_argument("--python_version", type=str, default="3.8.5")
    args = parser.parse_args()

    assert os.name == 'posix'
    check_system_requirements()
    atexit.register(at_exit)


    # must be replaced in another script
    NAME = '$name'
    VERSION = '$version'

    assert NAME != '$' + 'name'
    assert VERSION != '$' + 'version'


    PY_EMBED_URL = f'https://www.python.org/ftp/python/{args.python_version}/Python-{args.python_version}.tgz'
    GET_PIP_URL = 'https://bootstrap.pypa.io/get-pip.py'
    VC_REDIST_URL = 'https://aka.ms/vs/16/release/vc_redist.x64.exe'

    INSTALL_DIR = os.path.join(os.path.abspath(args.install_dir), NAME)

    PYTHON_SRC_DIR = os.path.join(INSTALL_DIR, f"Python-{args.python_version}")
    PYTHON_DIR = os.path.join(INSTALL_DIR, 'python')


    try:
        if os.path.exists(INSTALL_DIR):
            print(f"{INSTALL_DIR} is exist. it will be re-created")
            shutil.rmtree(INSTALL_DIR)
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

    for util in ("openssl", "zlib", "sqlite"):
        util_path = get_util_dir(util)
        if util_path not in os.getenv("CPPFLAGS", ""):
            os.environ["CPPFLAGS"] = "{} -I{}/include".format(os.getenv("CPPFLAGS", ""), util_path)
        if util_path not in os.getenv("LDFLAGS", ""):
            os.environ["LDFLAGS"] = "{} -L{}/lib".format(os.getenv("LDFLAGS", ""), util_path)
        if util in ("zlib", "sqlite"):
            os.environ["PKG_CONFIG_PATH"] = "{} {}/lib/pkgconfig".format(os.getenv("PKG_CONFIG_PATH", ""), util_path)
    config_cmd = "cd {};./configure --prefix={} --with-openssl={} --with-ssl-default-suites=openssl --silent".format(PYTHON_SRC_DIR, PYTHON_DIR, get_util_dir("openssl"))


    print("configuring python: {}".format(config_cmd))
    print("this may take a while...")
    print("CPPFLAGS: {}".format(os.getenv("CPPFLAGS", "")))
    print("LDFLAGS: {}".format(os.getenv("LDFLAGS", "")))
    print("PKG_CONFIG_PATH: {}".format(os.getenv("PKG_CONFIG_PATH", "")))
    status = os.system(config_cmd)
    if status:
        sys.exit("python configuring process finished with error(s)")
    print("installing python to %s" % PYTHON_DIR)
    print("this may take a while...")

    status = os.system("cd {}; make install --silent".format(PYTHON_SRC_DIR))
    shutil.rmtree(PYTHON_SRC_DIR)
    if status:
        sys.exit("installation process finished with error(s)")


    PYTHON_EXEC = os.path.join(PYTHON_DIR, 'bin/python3')
    print("python location: {}".format(PYTHON_EXEC))
    os.system("{} -V".format(PYTHON_EXEC))

    if os.system('{} -m pip install torch==1.5.0 torchvision==0.6.0 -f https://download.pytorch.org/whl/torch_stable.html --no-warn-script-location'.format(PYTHON_EXEC)):
        sys.exit("python packages installation finished with error(s)")
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
