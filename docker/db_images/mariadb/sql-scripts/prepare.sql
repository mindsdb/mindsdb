CREATE TABLE rentals (
number_of_rooms INT,
number_of_bathrooms INT,
sqft varchar(25),
location varchar(25),
days_on_market INT,
initial_price FLOAT,
neighborhood varchar(25),
rental_price FLOAT
);

LOAD DATA INFILE '/home_rentals.csv' INTO TABLE rentals COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;

CREATE USER 'ssl_user'@'172.17.0.1' IDENTIFIED BY 'ssl' REQUIRE SSL;
GRANT ALL ON *.* TO 'ssl_user'@'172.17.0.1';
FLUSH PRIVILEGES;
ALTER USER 'ssl_user'@'172.17.0.1' REQUIRE X509;
