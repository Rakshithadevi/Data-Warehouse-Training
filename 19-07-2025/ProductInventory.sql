create database pro;
use pro;
CREATE TABLE ProductInventory (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    Supplier VARCHAR(100),
    LastRestocked DATE
);
INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');
--  CRUD Operations
INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES (6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');
UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp';
DELETE FROM ProductInventory
WHERE ProductID = 2;
SELECT * FROM ProductInventory
ORDER BY ProductName ASC;
-- Sorting and Filtering
SELECT * FROM ProductInventory
ORDER BY Quantity DESC;
SELECT * FROM ProductInventory
WHERE Category = 'Electronics';
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20;
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000;
-- Aggregation and Grouping
SELECT SUM(Quantity * UnitPrice) AS TotalStockValue
FROM ProductInventory;
SELECT Category, AVG(UnitPrice) AS AvgPrice
FROM ProductInventory
GROUP BY Category;
SELECT COUNT(*) AS ProductCount
FROM ProductInventory
WHERE Supplier = 'GadgetHub';
-- Conditional and Pattern Matching
SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%';
SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000;
SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000;
-- Advanced Queries
SELECT * FROM ProductInventory
ORDER BY UnitPrice DESC
LIMIT 3;
SELECT * FROM ProductInventory
WHERE LastRestocked >= DATE('2025-05-10') - INTERVAL 10 DAY;
SELECT Supplier, SUM(Quantity) AS TotalQuantity
FROM ProductInventory
GROUP BY Supplier;
SELECT * FROM ProductInventory
WHERE Quantity < 30;
-- Join and Subqueries
WITH SupplierCounts AS (
    SELECT Supplier, COUNT(*) AS ProductCount
    FROM ProductInventory
    GROUP BY Supplier
)
SELECT s.*
FROM SupplierCounts s
JOIN (
    SELECT MAX(ProductCount) AS MaxCount FROM SupplierCounts
) m ON s.ProductCount = m.MaxCount;
WITH StockValues AS (
    SELECT ProductID, ProductName, Quantity * UnitPrice AS StockValue
    FROM ProductInventory
)
SELECT pi.*
FROM ProductInventory pi
JOIN (
    SELECT ProductID
    FROM StockValues
    ORDER BY StockValue DESC
    LIMIT 1
) top ON pi.ProductID = top.ProductID;
















