import os
import sys


os.system('mkdir -p dist')
installer_version = '1.5'

with open('Dockerfile.template', 'r') as fp:
    content = fp.read()
    content = content.replace('@@beta_or_release', sys.argv[1])

with open('dist/Dockerfile', 'w') as fp:
    fp.write(content)

if sys.argv[1] == 'release':
    container_name = 'mindsdb'
else:
    container_name = 'mindsdb_beta'
print(f"""
        Build, tag publish:
        cd dist &&
        sudo docker build -t {container_name} . &&
        sudo docker tag {container_name} mindsdb/{container_name}:latest &&
        sudo docker tag {container_name} mindsdb/{container_name}:{installer_version} &&
        sudo docker push mindsdb/{container_name};
        cd ..
      """)
