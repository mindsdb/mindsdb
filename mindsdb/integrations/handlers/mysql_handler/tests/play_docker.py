import time
import docker

connection_data = {
    "host": "localhost",
    "port": "3306",
    "user": "root",
    "password": "supersecret",
    "database": "test",
    "ssl": False
}

kwargs = {'connection_data': connection_data}
client = docker.from_env()
# cr = client.containers.run(
#     "my-mysql",
#     command="--secure-file-priv=/",
#     detach=True,
#     environment={"MYSQL_ROOT_PASSWORD":"supersecret"},
#     ports={"3306/tcp": 3307},
#     # remove=True,
#     )

cr = client.containers.get("hopeful_bhaskara")

with open("certs.tar", "wb") as f:
    bits, stats = cr.get_archive('/var/lib/mysql')
    print(stats)
    for chunk in bits:
        f.write(chunk)
