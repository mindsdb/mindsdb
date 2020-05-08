import os
import sys
import time
import json
from subprocess import Popen, PIPE

instance_id = 'i-0d6aa6ae788b28ccb'

output = Popen(['aws', 'ec2', 'start-instances', '--instance-ids' ,instance_id],stdout=PIPE)
response = output.communicate()

print('Booted up instance, waiting 2 minutes for it to start')
time.sleep(120)

output = Popen(['aws', 'ec2', 'describe-instances', '--instance-ids' ,instance_id],stdout=PIPE)
response = output.communicate()

j = json.loads(response[0])
status = j['Reservations'][0]['Instances'][0]['State']['Name']
ip = j['Reservations'][0]['Instances'][0]['PublicIpAddress']

os.system(f'ssh -t ubuntu@{ip} \' sudo /var/benchmarks/mindsdb/tests/accuracy_benchmarking/launch_server.sh {sys.argv[1]} {sys.argv[2]} {sys.argv[3]}\'')
