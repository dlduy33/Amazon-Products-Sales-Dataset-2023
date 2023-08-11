--Create Cassandra keyspace
CREATE KEYSPACE IF NOT EXISTS datalake WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE datalake;

--Create Tables following database schema
CREATE TABLE IF NOT EXISTS datalake.inventory(
    id INT PRIMARY KEY,
    quantity INT,
    quantity_sold INT
);

CREATE TABLE IF NOT EXISTS datalake.product(
    id INT PRIMARY KEY,
    product_name VARCHAR,
    category VARCHAR,
    type VARCHAR,
    actual_price INT,
    discount_price INT,
    ratings INT,
    no_of_ratings INT,
    link VARCHAR,
    image VARCHAR,
    inventory_id INT,
    order_id INT
);

CREATE TABLE IF NOT EXISTS datalake.orderr(
    id INT PRIMARY KEY,
    customer_id INT,
    order_date TIMESTAMP,
    ship_date TIMESTAMP,
    timestamp TIMESTAMP,
    timeuuid VARCHAR,
    quarter INT,
    delivery_status VARCHAR,
    city_id INT
);

CREATE TABLE IF NOT EXISTS datalake.customer(
    id INT PRIMARY KEY,
    customer_name VARCHAR,
    gender VARCHAR,
    email VARCHAR,
    segment VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.address(
    id INT PRIMARY KEY,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR
);

CREATE TABLE IF NOT EXISTS datalake.customerAddress(
    customer_id INT,
    address_id INT,
    PRIMARY KEY (customer_id, address_id)
);
