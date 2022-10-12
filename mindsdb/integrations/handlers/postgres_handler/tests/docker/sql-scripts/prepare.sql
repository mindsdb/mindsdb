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

COPY rentals FROM '/home_rentals.csv' DELIMITER ',' CSV HEADER;
