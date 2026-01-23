DROP DATABASE IF EXISTS mdb_db_handler_test;
CREATE DATABASE mdb_db_handler_test;

USE mdb_db_handler_test;
START TRANSACTION;


CREATE TABLE test (
    col_one INT,
    col_two INT,
    col_three FLOAT,
    col_four VARCHAR(20)
);


INSERT INTO test VALUES (1, -1, 0.1, 'A');
INSERT INTO test VALUES (2, -2, 0.2, 'B');
INSERT INTO test VALUES (3, -3, 0.3, 'C');
COMMIT;