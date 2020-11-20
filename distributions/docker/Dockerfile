FROM pytorch/pytorch:latest

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    pip install git+https://github.com/mindsdb/mindsdb.git@staging

ENV PYTHONUNBUFFERED=1

RUN echo '{' \
    '"config_version":"1.3",' \
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
        '},' \
        '"mongodb": {' \
            '"host": "0.0.0.0",' \
            '"port": "47336",' \
            '"database": "mindsdb"' \
        '}' \
    '}' \
'}' > /root/mindsdb_config.json

CMD ["python", "-m", "mindsdb", "--api=http,mysql", "--config=/root/mindsdb_config.json"]
