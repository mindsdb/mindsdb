FROM python:3.10 as builder

ARG EXTRAS

COPY . /mindsdb
WORKDIR /mindsdb
RUN pip install --no-cache-dir "."
RUN pip install --no-cache-dir ${EXTRAS} || true



FROM python:3.10-slim

RUN apt update && apt-get upgrade -y && apt-get install -y libmagic1 libpq5 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages

ENV FLASK_DEBUG "1"

EXPOSE 47334/tcp
EXPOSE 47335/tcp
EXPOSE 47336/tcp
