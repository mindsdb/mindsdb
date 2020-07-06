FROM mariadb:10.3

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
        mariadb-plugin-connect

RUN chmod 0444 /etc/mysql/mariadb.conf.d/connect.cnf

VOLUME /var/lib/mysql

EXPOSE 3306

CMD ["mysqld"]
