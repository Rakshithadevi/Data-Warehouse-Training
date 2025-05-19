create database book1;
use book1;
CREATE TABLE Book (
    BookID INT PRIMARY KEY,
    Title VARCHAR(255),
    Author VARCHAR(255),
    Genre VARCHAR(100),
    Price DECIMAL(10, 2),
    PublishedYear INT,
    Stock INT
);
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock) 
VALUES
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);
--- CRUD Operations
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock)
VALUES (6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);

UPDATE Book
SET Price = Price + 50
WHERE Genre = 'Self-Help';

DELETE FROM Book
WHERE BookID = 4;

SELECT * FROM Book
ORDER BY Title ASC;

--- Sorting and Filtering
SELECT * FROM Book
ORDER BY Price DESC;

SELECT * FROM Book
WHERE Genre = 'Fiction';

SELECT * FROM Book
WHERE Genre = 'Self-Help' AND Price > 400;

SELECT * FROM Book
WHERE Genre = 'Fiction' OR PublishedYear > 2000;

--- Aggregation and Grouping

SELECT SUM(Price * Stock) AS TotalStockValue FROM Book;

SELECT Genre, AVG(Price) AS AveragePrice
FROM Book
GROUP BY Genre;

SELECT COUNT(*) AS BookCount
FROM Book
WHERE Author = 'Paulo Coelho';

--- Conditional and Pattern Matching

SELECT * FROM Book
WHERE Title LIKE '%The%';

SELECT * FROM Book
WHERE Author = 'Yuval Noah Harari' AND Price < 600;

SELECT * FROM Book
WHERE Price BETWEEN 300 AND 500;
--- Advanced Queries

SELECT * FROM Book
ORDER BY Price DESC
LIMIT 3;

SELECT * FROM Book
WHERE PublishedYear < 2000;

SELECT Genre, COUNT(*) AS TotalBooks
FROM Book
GROUP BY Genre;

SELECT Title, COUNT(*) AS Count
FROM Book
GROUP BY Title
HAVING COUNT(*) > 1;
--- Join and Subqueries (Advanced)

SELECT Author, BookCount
FROM (SELECT Author, COUNT(*) AS BookCount
    FROM Book
    GROUP BY Author) AS AuthorCounts
WHERE BookCount = (
    SELECT MAX(BookCount)
    FROM (SELECT COUNT(*) AS BookCount
        FROM Book
        GROUP BY Author) AS Sub
);

SELECT b.Genre, b.Title, b.PublishedYear
FROM Book b
JOIN (SELECT Genre, MIN(PublishedYear) AS MinYear
    FROM Book
    GROUP BY Genre) AS sub
ON b.Genre = sub.Genre AND b.PublishedYear = sub.MinYear;






