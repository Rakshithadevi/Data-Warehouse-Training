create database pro_db;
use  pro_db;
create table product (
    productid int primary key,
    productname varchar(100),
    category varchar(50),
    price decimal(10, 2),
    stockquantity int,
    supplier varchar(100)
);
insert into product (productid, productname, category, price, stockquantity, supplier)
values(1, 'laptop', 'electronics', 70000, 50, 'techmart'),
(2, 'office chair', 'furniture', 5000, 100, 'homecomfort'),
(3, 'smartwatch', 'electronics', 15000, 200, 'gadgethub'),
(4, 'desk lamp', 'lighting', 1200, 300, 'brightlife'),
(5, 'wireless mouse', 'electronics', 1500, 250, 'gadgethub');
-- 1. CRUD Operations:
insert into product (productid, productname, category, price, stockquantity, supplier)
values (6, 'gaming keyboard', 'electronics', 3500, 150, 'techmart');
select * from product;
update product
set price = price * 1.10
where category = 'electronics';
delete from product
where productid = 4;
-- 2. Sorting and Filtering:
select * from product
order by price desc;
select * from product
order by stockquantity asc;
select * from product
where category = 'electronics';
select * from product
where category = 'electronics' and price > 5000;
select * from product
where category = 'electronics' or price < 2000;
-- 3. Aggregation and Grouping:
select sum(price * stockquantity) as totstockval
from product;
select category, avg(price) as avgprice
from product
group by category;
select count(*) as procount
from product
where supplier = 'gadgethub';
-- 4. Conditional and Pattern Matching:
select * from product
where productname like '%wireless%';
select * from product
where supplier in ('techmart', 'gadgethub');
select * from product
where price between 1000 and 20000;
select * from product
where stockquantity > (
    select avg(stockquantity) from product
);
select * from product
order by price desc
limit 3;
select supplier, count(*) as count
from product
group by supplier
having count(*) > 1;
select category, count(*) as prodcount, sum(price * stockquantity) as totstockvals
from product
group by category;
-- 5. Advanced Queries:
with supplierproductcount as (
    select supplier, count(*) as procount
    from product
    group by supplier
)
select s.supplier, s.procount
from supplierproductcount s
join (
    select max(procount) as maxcount
    from supplierproductcount
) max_sup on s.procount = max_sup.maxcount;
with maxpricepercategory as (
    select category, max(price) as maxprice
    from product
    group by category
)
select p.*
from product p
join maxpricepercategory m
  on p.category = m.category and p.price = m.maxprice;





















