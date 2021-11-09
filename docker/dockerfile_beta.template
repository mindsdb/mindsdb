FROM pytorch/pytorch:latest

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y curl && \
    pip install mindsdb==@@installer_version;

ENV PYTHONUNBUFFERED=1

RUN echo '{' \
    '"config_version":"1.4",' \
    '"storage_dir": "/root/mdb_storage",' \
    '"log": {' \
        '"level": {' \
            '"console": "ERROR",' \
            '"file": "WARNING"' \
        '}' \
    '},' \
    '"debug": false,' \
    '"integrations": {},' \
    '"api": {' \
        '"http": {' \
            '"host": "0.0.0.0",' \
            '"port": "47334"' \
        '},' \
        '"mysql": {' \
            '"host": "0.0.0.0",' \
            '"password": "",' \
            '"port": "47335",' \
            '"user": "mindsdb",' \
            '"database": "mindsdb",' \
            '"ssl": true' \
        '}', \
         '"mongodb": {' \
            '"host": "0.0.0.0",' \
            '"port": "47336",' \
            '"database": "mindsdb"' \
        '}' \
    '}' \
'}' > /root/mindsdb_config.json

CMD bash -c 'if [ -n "$MDB_CONFIG_CONTENT" ]; then echo "$MDB_CONFIG_CONTENT" > /root/mindsdb_config.json; fi; pip install mindsdb==$(curl https://public.api.mindsdb.com/installer/@@beta_or_release/docker___started___None) && python -m mindsdb --config=/root/mindsdb_config.json --api=http,mysql,mongodb'