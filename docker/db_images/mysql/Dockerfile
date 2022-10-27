FROM mysql

ENV MYSQL_DATABASE test

COPY ./sql-scripts/ /docker-entrypoint-initdb.d/
COPY home_rentals.csv /
