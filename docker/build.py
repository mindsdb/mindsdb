#!/bin/env python3

import os
import sys
import requests
from shutil import copy


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
    container_name = 'mindsdb'
    dockerfile = 'release'

elif sys.argv[1] == 'beta':
    container_name = 'mindsdb_beta'
    dockerfile = 'beta'

print(f"""
    Build, tag publish:

      docker build -f {dockerfile} {build_arg} -t {container_name} . &&
      docker tag {container_name} mindsdb/{container_name}:latest &&
      docker tag {container_name} mindsdb/{container_name}:{installer_version} &&
      docker push mindsdb/{container_name};
""")
