#!/bin/env python3

import sys
import requests
import subprocess


if not sys.argv[1:]:
    sys.exit("usage: build.py <beta|release>")

reltype = sys.argv[1]

installer_version_url = f'https://public.api.mindsdb.com/installer/{reltype}/docker___success___None'

api_response = requests.get(installer_version_url)

if api_response.status_code != 200:
    exit(1)

installer_version = api_response.text

build_arg = f'--build-arg VERSION={installer_version}'

if sys.argv[1] == 'release':
    container_name = 'mindsdb/mindsdb'
    dockerfile = 'release'

elif sys.argv[1] == 'beta':
    container_name = 'mindsdb/mindsdb_beta'
    dockerfile = 'beta'

print(installer_version)
command = (f"""
        docker build -f {dockerfile} {build_arg} -t {container_name}:latest -t {container_name}:{installer_version} . &&
        docker push {container_name} --all-tags
      """)

subprocess.run(command, shell=True, check=True)
