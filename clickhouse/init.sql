CREATE USER IF NOT EXISTS aldarbis IDENTIFIED WITH plaintext_password BY '1111';
--GRANT ALL ON *.* TO aldarbis;

CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id UUID,
    customer_id UUID,
    order_date Date,
    amount Float32
) ENGINE = MergeTree()
ORDER BY order_date;

