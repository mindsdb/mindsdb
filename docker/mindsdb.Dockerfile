FROM python:3.8

ARG EXTRAS

RUN apt update && apt-get upgrade -y && apt install -y build-essential libxml2 libmagic1
RUN python3 -m pip install --no-cache-dir --upgrade pip

COPY mindsdb /mindsdb
WORKDIR /mindsdb

RUN pip install "."
RUN pip install ${EXTRAS} || true

ENV PYTHONPATH "/mindsdb"
ENV FLASK_DEBUG "1"

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
