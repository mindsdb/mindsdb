# TODO set -e to error if any fails
# ###########
# View releases...
# Note: Using the "python-versions" code to replicate what we run in CI/CD locally, first check for the URL for the latest of the version(s) you wish to test...
#       See: https://raw.githubusercontent.com/actions/python-versions/main/versions-manifest.json
#
# Latest 3.8 (as of October 25, 2023)
#     URL: https://github.com/actions/python-versions/releases/download/3.8.18-5997368067/python-3.8.18-linux-22.04-x64.tar.gz
# Latest 3.9 (as of October 25, 2023)
#     URL: https://github.com/actions/python-versions/releases/download/3.9.18-5997508477/python-3.9.18-linux-22.04-x64.tar.gz
# ###########
```bash
# Startup Docker (daemonized...)
docker run --name ubuntu-2204-mindsdb-test -d --entrypoint "sleep" -v `pwd`:/app ubuntu:22.04 99999999999
# Startup Docker (interactive, cleanup automatically after)
docker run --rm -ti --entrypoint "sleep" -v `pwd`:/app ubuntu:22.04 99999999999
# Setup environment, install necessary software
export DEBIAN_FRONTEND=noninteractive
apt-get update
# IF USING DOCKER for services...
apt-get install wget joe git sqlite3 ca-certificates curl libmagic-dev sudo tzdata -y

# IF NOT USING DOCKER AND WANT TO RUN ALL DEPS ON THIS SERVER THEN ADD THESE...
apt-get install redis-server postgresql mysql-server -y
# Run mysql but without grants, make it easier to use for testing
service mysql stop
echo "ALTER USER 'root'@'localhost' IDENTIFIED BY 'root';" > /tmp/init.sql
sudo chown mysql:mysql /tmp/init.sql
mysqld --init-file=/tmp/init.sql --console
echo -e "[mysqld]\nskip-grant-tables\nskip-networking=0\nbind-address=0.0.0.0" > /etc/mysql/conf.d/mysqld.cnf
service mysql start
# Start redis
service redis-server start
# Configure and start postgres without grants, making it easier to use for testing
cat > /etc/postgresql/14/main/pg_hba.conf << 'EOF'
# This sets to "trust" all connections to pg, only useful for development/local/testing/etc, do not use this for anything else
host    all             all             0.0.0.0/0               trust
local   all             all                                     trust
EOF
service postgresql start

# Seed databases...
TODO?

# Install python...
OLDPWD=`pwd`

# 3.8
export PY_VERSION="3.8.18"
export pythonLocation=/opt/hostedtoolcache/Python/${PY_VERSION}/x64
# 3.9
export PY_VERSION="3.9.18"
export pythonLocation=/opt/hostedtoolcache/Python/${PY_VERSION}/x64
# 3.10 (todo)
# 3.11 (todo)


# Actually download the desired version
mkdir -p $pythonLocation
wget https://github.com/actions/python-versions/releases/download/${PY_VERSION}-5997508477/python-${PY_VERSION}-linux-22.04-x64.tar.gz -O /tmp/python.tar.gz
tar -xf /tmp/python.tar.gz -C $pythonLocation
cd $pythonLocation/bin
ln -s pip3 pip
cd $OLDPWD

export PY_VERSION="3.9.18"
export pythonLocation=/opt/hostedtoolcache/Python/${PY_VERSION}/x64
# Setup env vars to enable/use it
export PKG_CONFIG_PATH=$pythonLocation/lib/pkgconfig
export LD_LIBRARY_PATH=$pythonLocation/lib
export PATH=$pythonLocation/bin:$PATH

# Try it out...
pip3 --version
python3 --version

## Install dependencies - copied from .github/workflows/mindsdb.yml and/or from MindsDB Gateway AMI builder
export PIP_CACHE_DIR=/root/.cache/pip/download-cache


mkdir -p $PIP_CACHE_DIR
python3 -m pip install --cache-dir="${PIP_CACHE_DIR}" pip===23.0
python3 -m pip install --upgrade --download-cache="/pth/to/downloaded/files" setuptools wheel

# pip install mindsdb package...
# Download packages first for caching purposes
pip download . -d $PIP_CACHE_DIR
# Install them from cache links if possible
pip install --find-links=$PIP_CACHE_DIR .

# pip install mindsdb test package(s)
pip download -r requirements_test.txt -d $PIP_CACHE_DIR
pip install --find-links=$PIP_CACHE_DIR -r requirements_test.txt

# Post-install staging branches to ensure we have the latest versions, if needed/desired
# pip install git+https://github.com/mindsdb/mindsdb_sql.git@staging --upgrade
# pip install git+https://github.com/mindsdb/lightwood.git@staging --upgrade

# Install ML engine related dependencies, using cache
for engine in openai anyscale_endpoints; do  # statsforecast huggingface lightwood timegpt (Note: timegpt does not work, fix please)
    pip download -r ./mindsdb/integrations/handlers/${engine}_handler/requirements.txt -d $PIP_CACHE_DIR
    pip install --find-links=$PIP_CACHE_DIR -r ./mindsdb/integrations/handlers/${engine}_handler/requirements.txt
done

# Run tests
# Run integration api and flow tests
# Unit tests
# echo -e "\n===============\nUnit tests\n===============\n"
export PYTHONPATH=./
pytest tests/unit/test_executor.py
pytest tests/unit/test_project_structure.py
pytest tests/unit/test_predictor_params.py
pytest tests/unit/test_mongodb_handler.py
pytest tests/unit/test_mongodb_server.py
pytest tests/unit/test_cache.py

# ERROR: REQUIRES DOCKER (fixes with seed-tests.sql + redis + postgres)
pytest -vx tests/integration_tests/flows/test_ml_task_queue.py

# ERROR: REQUIRES DOCKER (NO FIX IN PLACE YET)
# MySQL API
echo -e "\n===============test MySQL API===============\n"
pytest -vx tests/integration_tests/flows/test_mysql_api.py

# ERROR: REQUIRES DOCKER (NO FIX IN PLACE YET)
# MySQL binary API
echo -e "\n===============test MySQL binary API===============\n"
pytest -vx -k 'not TestMySqlApi' tests/integration_tests/flows/test_mysql_bin_api.py

# ERROR: REQUIRES DOCKER (fixes with seed-tests.sql + redis + postgres)
# echo -e "\n===============TS predictions===============\n"
pytest -svx tests/integration_tests/flows/test_ts_predictions.py

# ERROR: REQUIRES DOCKER (fixes with seed-tests.sql + redis + postgres)
# HTTP
echo -e "\n===============test HTTP===============\n"
pytest -vx tests/integration_tests/flows/test_http.py

# ERROR: REQUIRES DOCKER...
# Company independent
echo -e "\n===============test company independent===============\n"
pytest -vx tests/integration_tests/flows/test_company_independent.py

# Does not require docker
# First-tier ML engines
echo -e "\n===============test ML engines===============\n"
# ERROR: REQUIRES API KEY - OPENAI_API_KEY
pytest -vx tests/unit/ml_handlers/test_openai.py
# pytest -vx tests/unit/ml_handlers/test_timegpt.py  # Note: timegpt doesn't work, fix please
# ERROR: REQUIRES API KEY - ANYSCALE_ENDPOINTS_API_KEY
pytest -vx tests/unit/ml_handlers/test_anyscale_llm.py


export DOCKER_CLI_VERSION="20.10.6" # please replace with your desired version
export DOCKER_CLI_DOWNLOAD_URL="https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz"
wget $DOCKER_CLI_DOWNLOAD_URL -O /tmp/docker.tgz
tar xzvf /tmp/docker.tgz --strip 1 -C /usr/local/bin docker/docker


DOCKER_VERSION=24.0.6
DOCKER_TLS_CERTDIR=/certs
DOCKER_BUILDX_VERSION=0.11.2
DOCKER_COMPOSE_VERSION=2.23.0


docker run --privileged --name some-docker -d docker:dind


docker-init -- dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2376 --tlsverify --tlscacert /certs/server/ca.pem --tlscert /certs/server/cert.pem --tlskey /certs/server/key.pem
containerd --config /var/run/docker/containerd/containerd.toml

```










```bash
/ # cat /var/run/docker/containerd/containerd.toml
disabled_plugins = ["io.containerd.grpc.v1.cri"]
imports = []
oom_score = 0
plugin_dir = ""
required_plugins = []
root = "/var/lib/docker/containerd/daemon"
state = "/var/run/docker/containerd/daemon"
temp = ""
version = 2

[cgroup]
  path = ""

[debug]
  address = "/var/run/docker/containerd/containerd-debug.sock"
  format = ""
  gid = 0
  level = ""
  uid = 0

[grpc]
  address = "/var/run/docker/containerd/containerd.sock"
  gid = 0
  max_recv_message_size = 16777216
  max_send_message_size = 16777216
  tcp_address = ""
  tcp_tls_ca = ""
  tcp_tls_cert = ""
  tcp_tls_key = ""
  uid = 0

[metrics]
  address = ""
  grpc_histogram = false

[plugins]

[proxy_plugins]

[stream_processors]

[timeouts]

[ttrpc]
  address = ""
  gid = 0
  uid = 0
```