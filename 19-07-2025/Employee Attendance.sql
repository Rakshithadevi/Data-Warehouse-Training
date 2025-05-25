create database emp;
use emp;
CREATE TABLE EmployeeAttendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    Department VARCHAR(50),
    Date DATE,
    Status VARCHAR(20),
    HoursWorked INT
);
INSERT INTO EmployeeAttendance (AttendanceID, EmployeeName, Department, Date, Status, HoursWorked)
VALUES
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);

-- 1. CRUD Operations:
insert into EmployeeAttendance (AttendanceID, EmployeeName, Department, Date, Status, HoursWorked)
VALUES (6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);
update EmployeeAttendance
set Status="Present"
where EmployeeName='Riya Patel';
delete from EmployeeAttendance
where EmployeeName="Priya Singh" and Date="2025-05-01";
select * from EmployeeAttendance
order by EmployeeName asc;

-- 2. Sorting and Filtering:
select * from EmployeeAttendance
order by HoursWorked desc;
select * from EmployeeAttendance
where Department="IT";
select * from EmployeeAttendance
where Status="Present" and Department="IT";
select * from EmployeeAttendance
where Status="Absent" or Status="Late";

3. Aggregation and Grouping:
select Department, sum(HoursWorked) as tothourwork
from EmployeeAttendance
group by Department;
select AVG(HoursWorked) AS AverageHoursWorked
from EmployeeAttendance;
select Status, COUNT(*) AS Count
from EmployeeAttendance
group by Status;

-- 4. Conditional and Pattern Matching:
select * from EmployeeAttendance
where EmployeeName LIKE 'R%';
select * from EmployeeAttendance
where HoursWorked > 6 AND Status = 'Present';
SELECT * FROM EmployeeAttendance
WHERE HoursWorked BETWEEN 6 AND 8;

-- 5. Advanced Queries:
SELECT * FROM EmployeeAttendance
ORDER BY HoursWorked DESC
LIMIT 2;
SELECT * FROM EmployeeAttendance
WHERE HoursWorked < (SELECT AVG(HoursWorked) FROM EmployeeAttendance);
SELECT Status, AVG(HoursWorked) AS AverageHours
FROM EmployeeAttendance
GROUP BY Status;
SELECT EmployeeName, Date, COUNT(*) AS EntryCount
FROM EmployeeAttendance
GROUP BY EmployeeName, Date
HAVING COUNT(*) > 1;

-- 6. Join and Subqueries 
WITH DepartmentPresentCounts AS (
    SELECT Department, COUNT(*) AS PresentCount
    FROM EmployeeAttendance
    WHERE Status = 'Present'
    GROUP BY Department
)
SELECT d1.Department, d1.PresentCount
FROM DepartmentPresentCounts d1
JOIN (
    SELECT MAX(PresentCount) AS MaxPresent
    FROM DepartmentPresentCounts
) d2 ON d1.PresentCount = d2.MaxPresent;

SELECT ea.*
FROM EmployeeAttendance ea
JOIN (
    SELECT Department, MAX(HoursWorked) AS MaxHours
    FROM EmployeeAttendance
    GROUP BY Department
) max_per_dept
ON ea.Department = max_per_dept.Department AND ea.HoursWorked = max_per_dept.MaxHours;













