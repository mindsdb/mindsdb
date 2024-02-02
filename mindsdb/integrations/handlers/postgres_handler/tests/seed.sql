

-- Create the table
CREATE TABLE test (
    col_one INT,
    col_two INT,
    col_three FLOAT,
    col_four VARCHAR(20)
);

-- Insert data into the table
INSERT INTO test (col_one, col_two, col_three, col_four)
VALUES (1, -1, 0.1, 'A');
INSERT INTO test (col_one, col_two, col_three, col_four)
VALUES (2, -2, 0.2, 'B');
INSERT INTO test (col_one, col_two, col_three, col_four)
VALUES (3, -3, 0.3, 'C');

COMMIT;
