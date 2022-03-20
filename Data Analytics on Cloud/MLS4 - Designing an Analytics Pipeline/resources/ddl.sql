DROP DATABASE IF EXISTS retail;

CREATE DATABASE retail;
USE retail;

DROP TABLE IF EXISTS T_Transactions;
CREATE TABLE T_Transactions (
	Invoice VARCHAR(60),
	StockCode VARCHAR(60),
	Description VARCHAR(60),
	Quantity INTEGER,
	InvoiceDate DATETIME,
	Price DECIMAL,
	CustomerID INTEGER,
	Country VARCHAR(60),
	Revenue DECIMAL
);