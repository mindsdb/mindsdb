FROM python:3.8


RUN apt update && apt-get upgrade -y && apt install -y build-essential

# db2 requirement
RUN apt install -y libxml2 libmagic1 || true

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir boto3 psycopg2-binary 

WORKDIR /
# COPY requirements.txt /requirements.txt

COPY . /mindsdb/
WORKDIR /mindsdb
# RUN pip install ".[grpc]" ".[telemetry]"

RUN pip install git+https://github.com/mindsdb/lightwood.git@staging --upgrade --no-cache-dir
RUN python3 -c 'import nltk; nltk.download("punkt");'
RUN pip install neuralforecast
# COPY ./mindsdb /mindsdb/mindsdb

ENV PYTHONPATH "/mindsdb"
ENV FLASK_DEBUG "1"

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
