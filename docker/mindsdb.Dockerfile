FROM python:3.7


RUN apt update && apt-get upgrade -y && apt install -y build-essential

# db2 requirement
RUN apt install -y libxml2 || true

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir boto3

WORKDIR /
COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt --force-reinstall
RUN pip install git+https://github.com/mindsdb/lightwood.git@staging --upgrade --no-cache-dir
COPY ./mindsdb /mindsdb/mindsdb

ENV PYTHONPATH "/mindsdb"
ENV FLASK_DEBUG "1"

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
