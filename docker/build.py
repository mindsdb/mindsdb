import os
import sys
import requests
import subprocess

installer_version_url = 'https://public.api.mindsdb.com/installer/@@beta_or_release/docker___success___None'

api_response = requests.get(
    installer_version_url.replace('@@beta_or_release', sys.argv[1]))

if api_response.status_code != 200:
    exit(1)

installer_version = api_response.text

os.system('mkdir -p dist')

if sys.argv[1] == 'release':
    container_name = 'mindsdb'
    dockerfile_template = 'dockerfile_release.template'

elif sys.argv[1] == 'beta':
    container_name = 'mindsdb_beta'
    dockerfile_template = 'dockerfile_beta.template'

with open(dockerfile_template, 'r') as fp:
    content = fp.read()
    content = content.replace('@@beta_or_release', sys.argv[1])
    content = content.replace('@@installer_version', installer_version)

with open('dist/Dockerfile', 'w') as fp:
    fp.write(content)

print(installer_version)
command = (f"""
        cd dist &&
        docker build -t mindsdb/{container_name}:latest -t mindsdb/{container_name}:{installer_version} . &&
        docker push mindsdb/{container_name} --all-tags
      """)

subprocess.run(command, shell=True, check=True)