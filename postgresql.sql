-- find duplicate rows in postgresql table

select student_id, count(student_id)
from tbl_scores 
group by student_id
having count(student_id) > 1
order by student_id;

-- delete duplicate rows in postgresql table

DELETE FROM tbl_scores
WHERE student_id IN
(SELECT student_id
FROM
(SELECT student_id,
ROW_NUMBER() OVER(PARTITION BY student_id
ORDER BY student_id) AS row_num
FROM tbl_scores) t
WHERE t.row_num > 1);

-- create commands to setup postgresql table

create table v1.tbl_students (
    student_id serial primary key,
    full_name varchar not null,
    teacher_id integer, 
    department varchar not null
);

create table v1.tbl_teachers (
    teacher_id serial primary key,
    full_name varchar not null,
    department_id varchar not null
);

create table v1.tbl_departments (
    department_id varchar not null,
    teacher_id serial primary key,
    department_name varchar not null
);

create table v1.tbl_scores (
    student_id varchar not null,
    teacher_id serial primary key,
    department_id varchar not null,
    score varchar not null
);

-- insert data into a postgresql table

insert into v1.tbl_students (
    student_id, full_name, teacher_id, department_id
)
values 
(1, 'elvis wilson', null, 5),
(2, 'Cynthia Hilbert', 1, 7),
(3, 'Chadhuri Patel', 2, 5),
(4, 'Andre Kharlanov', 5, 9),
(5, 'Buster Chaplin', 2, 8);

-- How to Use PostgreSQL Recursive Queries

WITH RECURSIVE cohort AS ( 
    SELECT student_id, teacher_id, full_name
    from v1.tbl_students
    where student_id = 2
    UNION 
    SELECT e.student_id, e.teacher_id, e.full_name
    FROM v1.tbl_students e
    INNER JOIN cohort s ON s.student_id = e.teacher_id
)
select * from cohort;

-- PostgreSQL FETCH Command to Limit Query Results

select student_id, score
from v1.tbl_scores
order by student_id
fetch first row only;

-- Expert Inner Join Queries in PostgreSQL

select tbl_students.student_id, full_name
from tbl_students
inner join v1.tbl_scores on score.student_id = v1.tbl_students.student_id
order by tbl_students.student_id;

-- If we want only a specific student’s scores, we can add the WHERE clause like this:

	
WHERE
tbl_students.student_id = 3; 

-- Advanced PostgreSQL Self-Join Query and Alias

select 
s1.full_name,
s2.full_name,
s1.score,
from 
v1.tbl_scores s1  
inner join v1.tbl_scores s2 on s1.student_id <> s2.student_id
and s1.score = s2.score;

-- Full Outer Join Query

select student_name, department_name                  
from v1.tbl_departments e 
full outer join v1.tbl_departments d on d.department_id = e.department_id;

-- Advanced Where Clause in Full Outer Join Query

select student_name, department_name
from v1.tbl_students e 
full outer join v1.tbl_departments d on d.department_id = e.department_id
where 
student_name is null;

-- PostgreSQL Advanced Query Using the LEFT JOIN Clause

select 
v1.tbl_students.full_name, 
v1.tbl_students.student_id,        
v1.tbl_scores.student_id, 
v1.tbl_scores.score
from
tbl_students
left join v1.tbl_scores on v1.tbl_students.student_id = v1.tbl_scores.student_id;

-- Implementing the CROSS JOIN Query in PostgreSQL

create table labels (label char(1) primary key);
create table scores (score integer primary key);
insert into labels (label) 
values ('fahr'), ('cels');
insert into scores (score) 
values (1), (2);
select * from labels cross join scores;

-- Elegant NATURAL JOIN Query in PostgreSQL

select * from v1.tbl_students natural join v1.tbl_scores;

-- Applying the UNION Operator to Multiple PostgreSQL Advanced Queries

SELECT * FROM tbl_scores
UNION ALL
SELECT *
FROM tbl_departments
ORDER BY tbl_departments.full_name ASC;

-- Use a PostgreSQL Query Result to Create a Table

SELECT student_id, score
INTO v1.tbl_top_students
FROM v1.tbl_scores
WHERE score>AVG(score);

-- Implementing Math Functions in PostgreSQL Queries

SELECT AVG (score) FROM tbl_scores;

SELECT random() * 100 + 1 AS RAND_1_100;

-- Using the Advanced Subquery in PostgreSQL

SELECT student_id, score
FROM tbl_scores
WHERE score > (
SELECT AVG (score) FROM tbl_scores;
);

-- Querying Stats on the Postgre DB

	
SELECT relname, relpages FROM pg_class ORDER BY relpages DESC limit 1;

-- To understand the system level keywords in this query, have a look at this list:

-- relname – table name
-- relpages – number of pages
-- pg_class – system table names
-- limit – limits output to the first result

-- 

-- Using PostgreSQL SELECT DISTINCT clause

SELECT DISTINCT ON
student_id, score
FROM tbl_scores
WHERE score > (
SELECT AVG (score) FROM tbl_scores;
);

-- Add a custom function with Java!

Public Class App {
private final String url = "jdbc:postgresql://localhost/studentdb";
private final String username = "postgresql";
private final String pwd = "postgresql";
public Connection connect() throws SQLException {
return DriverManager.getConnection(url, username, pwd);
}
//...
}

-- Now, here is the standard Java code to call a stored procedure in the PostgreSQL Server:

public class App {
public String properCaseA1(String str1) {
String result = str1;
try (Connection conn = this.connect();
CallableStatement properCaseA1 = conn.prepareCall("{ ? = call initcap( ? ) }")) {
properCaseA1.registerOutParameter(1, Types.VARCHAR);
properCaseA1.setString(2, str1);
properCaseA1.execute();
result = properCaseA1.getString(1);
} catch (SQLException e) {
System.out.println(e.getMessage());
}
return result;
}
public static void main(String[] args) {
App app = new App();
System.out.println(app.properCaseA1("Student list follows:"));
}
}

-- We can call a stored procedure by name, or send the PostgreSQL code to the server. In this example we send the code to list students from our DB:

public void getStudents(String pattern, int score) {
String SQL = "SELECT * FROM tbl_students (?, ?)";
try (Connection conn = this.connect();
PreparedStatement pstmtF = conn.prepareStatement(SQL)) {
pstmtF.setString(1,pattern);
pstmtF.setInt(2,score);
ResultSet rs = pstmtF.executeQuery();
while (rs.next()) {
System.out.println(String.format("%s %d",
rs.getString("full_name"),
rs.getInt("score")));
}
} catch (SQLException e) {
System.out.println(e.getMessageText());
}
}



-- 1. ROLLUP

SELECT Product, Size, count(*)
FRO<strong>M</strong> orders
GROUP BY ROLLUP(Product, Size)

-- 2. CUBE

SELECT Product_Name, Size, Count(*)
FRO<strong>M</strong> sales
GROUP BY CUBE(Product_Name, Size)

-- 3. Index

CREATE INDEX index_name ON table_name;

-- # CREATE INDEX eno_index ON E<strong>M</strong>PLOYEE (Eno);

-- 4. HAVING Clause

SELECT [DISTINCT] <column_list>| <expr>
FRO<strong>M</strong> 
<table>[,
<table>][WHERE <cond>]
GROUP BY <column | expr>
[HAVING <cond>]
<cond>;

-- SELECT deptno, AVG(salary)
-- FROM emp
-- GROUP BY deptno
-- HAVING COUNT(*)>5;

-- 5. Triggers

CREATE [ CONSTRAINT ] TRIGGER name { BEFORE | AFTER | INSTEAD OF } { event [ OR ... ] }
ON table
[ FRO<strong>M</strong> referenced_table ]
[ NOT DEFERRABLE | [ DEFERRABLE ] { INITIALLY I<strong>MM</strong>EDIATE | INITIALLY DEFERRED } ]
[ FOR [ EACH ] { ROW | STATE<strong>M</strong>ENT } ]
[ WHEN ( condi ) ]
EXECUTE PROCEDURE functionname ( arg )

-- Example:

CREATE OR REPLACE FUNCTION demo()
RETURNS trigger AS
$$
BEGIN
INSERT INTO demo1(col1,col2,col3)
VALUES(NEW.col1,NEW.col2,current_date);
RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER demo_trigger
AFTER INSERT
ON demo1
FOR EACH ROW
EXECUTE PROCEDURE demo();

-- 6. Missing Values in a Sequence

SELECT  empno + 1
FRO<strong>M</strong> emp a
WHERE   NOT EXISTS
(
SELECT  NULL
FRO<strong>M</strong> emp b
WHERE   a.empno = b.empno + 1
)
ORDER BY empno;

-- 7. RANK() function

SELECT
product,
grp_name,
price,
RANK () OVER (
PARTITION BY grp_name
ORDER BY
price
)
FRO<strong>M</strong>
substances
INNER JOIN products USING (productid);


-- 8. DENSE_RANK()

SELECT
product,
grp_name,
price,
DENSE_RANK () OVER (
PARTITION BY grp_name
ORDER BY
price
)
FRO<strong>M</strong>
substances
INNER JOIN products USING (productid);

-- 9. FIRST_VALUE()

SELECT
product,
grp_name,
price,
FIRST_VALUE(price) OVER (
PARTITION BY grp_name
ORDER BY
price
) AS low_price
FRO<strong>M</strong>
substances
INNER JOIN products USING (productid);

-- 10. LAST_VALUE()

SELECT
product,
grp_name,
price,
LAST_VALUE(price) OVER (
PARTITION BY grp_name
ORDER BY
price RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS high_price
FRO<strong>M</strong>
substances
INNER JOIN products USING (productid);

-- 11 Explain Statement
-- The EXPLAIN statement in PostgreSQL displays the execution. This is the plan which planner creates for a written SQL statement. 

EXPLAIN SELECT * FROM emp1;
QUERY PLAN
-------------------------------------------------------------
Seq Scan on emp1 (cost=0.00..451.00 rows=15000 width=364)


-- 12. Create Role
-- PostgreSQL applies roles to describe user accounts. It doesn’t apply to the user like other relational database management systems. For example, in PostgreSQL, the normal user is identified using roles.

CREATE ROLE jeff
LOGIN
PASSWORD 'myPass1';

SELECT rolname FROM pg_roles;

-- 13. group roles

CREATE ROLE emp;

GRANT emp TO jeff;



