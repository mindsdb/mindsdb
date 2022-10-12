FROM postgres

ENV POSTGRES_DB test

COPY ./sql-scripts/ /docker-entrypoint-initdb.d/
COPY home_rentals.csv /
