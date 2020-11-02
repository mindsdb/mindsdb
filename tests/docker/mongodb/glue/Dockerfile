FROM ubuntu:18.04

RUN apt-get update && apt-get install -y wget gnupg2

RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list

RUN apt-get update && apt-get install -y mongodb-org-shell

RUN mkdir /waitforit && cd /waitforit && \
    wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh && \
    chmod +x wait-for-it.sh

VOLUME [ "/data" ]

ENTRYPOINT [ "/data/entrypoint.sh" ]