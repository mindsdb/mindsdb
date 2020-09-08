FROM postgres:12.3

RUN apt-get update && apt-get install -y \
    postgresql-12-mysql-fdw

RUN mkdir -p /docker-entrypoint-initdb.d && \
    echo 'CREATE EXTENSION mysql_fdw;' > /docker-entrypoint-initdb.d/init.sql
