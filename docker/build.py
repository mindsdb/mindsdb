#!/bin/env python3

import sys
import requests
import subprocess


def build_container(reltype):
    installer_version_url = f'https://public.api.mindsdb.com/installer/{reltype}/docker___success___None'

    try:
        api_response = requests.get(installer_version_url)
        api_response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(f'Error making API request: {err}')
        return

    installer_version = api_response.text
    build_arg = f'--build-arg VERSION={installer_version}'

    if reltype == 'release':
        container_name = 'mindsdb/mindsdb'
        dockerfile = 'release'

    elif reltype == 'beta':
        container_name = 'mindsdb/mindsdb_beta'
        dockerfile = 'beta'

    command = "docker system prune --all --force"
    command += f" && DOCKER_BUILDKIT=1 docker build -f {dockerfile} {build_arg} -t {container_name}:latest -t {container_name}:{installer_version} ."
    command += f" && docker push {container_name} --all-tags"

    subprocess.run(command, shell=True, check=True)


if __name__ == '__main__':
    if not sys.argv[1:]:
        sys.exit("usage: build.py <beta|release>")

    reltype = sys.argv[1]
    build_container(reltype)
