**SQL for Data engineering:**

SQL (Structured Query Language) is a powerful tool for data engineering tasks, enabling you to manipulate, transform, and manage data efficiently. Below, I'll provide some common SQL operations and tasks often used in data engineering workflows:


These are some essential SQL operations for data engineering tasks. Keep in mind that the actual SQL code you use may vary depending on the specific database system you are working with (e.g., MySQL, PostgreSQL, SQL Server, etc.). Always refer to the documentation of the database system you are using for more detailed information.


The `RANK()` function is a window function in SQL that assigns a unique rank to each row within a result set based on the specified ordering criteria. It is commonly used for ranking data based on a specific column's values, such as ranking employees by their salaries or students by their scores.
The basic syntax of the `RANK()` function is as follows:
```sql
RANK() OVER (PARTITION BY partition_expression ORDER BY sort_expression [ASC|DESC])

```
* `PARTITION BY`: (Optional) Divides the result set into partitions or groups based on the values of one or more columns. The ranking is calculated separately for each partition.
* `ORDER BY`: Specifies the column(s) based on which the ranking is determined. The result set is ordered before the ranking is assigned.
* `ASC` (default) or `DESC`: Defines the sort order. `ASC` stands for ascending (lowest values ranked first), and `DESC` stands for descending (highest values ranked first).
Let's look at an example to illustrate how the `RANK()` function works. Suppose we have a table named `students` with the following data:
```diff
+------+-------+-------+
| ID   | Name  | Score |
+------+-------+-------+
| 1    | Alice | 85    |
| 2    | Bob   | 90    |
| 3    | Carol | 78    |
| 4    | David | 90    |
| 5    | Emma  | 82    |
+------+-------+-------+

```
We want to rank the students based on their scores. If two students have the same score, they will receive the same rank, and the next rank will be skipped (no ties).
Here's how you can use the `RANK()` function to achieve this:
```sql
SELECT 
    ID,
    Name,
    Score,
    RANK() OVER (ORDER BY Score DESC) AS Ranking
FROM
    students;

```
The result would be:
```diff
+------+-------+-------+---------+
| ID   | Name  | Score | Ranking |
+------+-------+-------+---------+
| 2    | Bob   | 90    | 1       |
| 4    | David | 90    | 1       |
| 1    | Alice | 85    | 3       |
| 5    | Emma  | 82    | 4       |
| 3    | Carol | 78    | 5       |
+------+-------+-------+---------+

```
As you can see, students Bob and David have the same score (90), so they share the top rank (1). The next rank is 3, assigned to Alice, and so on.
Remember that the `RANK()` function is just one of the many window functions available in SQL. Other window functions like `DENSE_RANK()`, `ROW_NUMBER()`, and `NTILE()` provide additional ranking and analytical capabilities.


Sure! `DENSE_RANK()`, `ROW_NUMBER()`, and `NTILE()` are all window functions in SQL used for different ranking and analytical purposes. Let's explore each of them:

The `COUNT()`, `AVG()`, and `SUM()` functions are commonly used window functions in SQL for calculating aggregate values based on a specified window (partition) within a result set. These functions allow you to perform aggregate calculations over a group of rows rather than the entire table. The window is defined by the `PARTITION BY` clause.
Let's explore each of these window functions:


These window functions (`COUNT()`, `AVG()`, and `SUM()`) are powerful tools for performing analytical calculations over specific partitions of data within a result set, providing valuable insights into your data.

`FIRST_VALUE()` and `LAST_VALUE()` are window functions in SQL used to get the first and last values within a window (partition) of a result set, respectively. These functions are particularly useful for analyzing data based on specific ordering criteria and identifying the initial and final values within each partition.
Let's delve into each of these window functions:

The `LAG()` function is a window function in SQL that allows you to access the value of a specific row from the previous row within a result set, based on a defined order. This function is useful for comparing the current row's value with the value of the preceding row or performing calculations that involve the previous row's data.
The basic syntax of the `LAG()` function is as follows:
```sql
LAG(column, offset, default_value) OVER (PARTITION BY partition_expression ORDER BY sort_expression)

```
* `column`: The column from which you want to retrieve the previous row's value.* `offset`: The number of rows to look back. An `offset` of 1 indicates the previous row, 2 for two rows back, and so on.* `default_value`: (Optional) The value returned when the `LAG()` function reaches the first row or a partition boundary where the offset is beyond the available rows.Let's look at an example to illustrate how the `LAG()` function works. Consider a table named `sales` with the following data:
```yaml
+------+------------+--------+
| ID   |  Sale_Date  | Amount |
+------+------------+--------+
| 1    | 2023-07-01 | 100    |
| 2    | 2023-07-03 | 150    |
| 3    | 2023-07-02 | 120    |
| 4    | 2023-07-01 | 200    |
| 5    | 2023-07-03 | 180    |
+------+------------+--------+

```
To retrieve the previous day's sales amount for each row, we can use the `LAG()` function:
```sql
SELECT
    ID,
    Sale_Date,
    Amount,
    LAG(Amount, 1) OVER (ORDER BY Sale_Date) AS Previous_Day_Sales
FROM
    sales;

```
Result:
```yaml
+------+------------+--------+--------------------+
| ID   | Sale_Date  | Amount | Previous_Day_Sales |
+------+------------+--------+--------------------+
| 1    | 2023-07-01 | 100    | NULL               |
| 4    | 2023-07-01 | 200    | 100                |
| 3    | 2023-07-02 | 120    | 200                |
| 2    | 2023-07-03 | 150    | 120                |
| 5    | 2023-07-03 | 180    | 150                |
+------+------------+--------+--------------------+

```
In this example, the `LAG()` function returns the previous day's sales amount for each row, ordered by the `Sale_Date`. The first row has a `NULL` value for the `Previous_Day_Sales` because there is no previous row to reference.
The `LAG()` function is helpful for performing time-series analysis, calculating differences between consecutive rows, identifying trends, and other types of sequential data processing in SQL.


The `LEAD()` function is a window function in SQL that allows you to access the value of a specific row from the following row within a result set, based on a defined order. It is the counterpart of the `LAG()` function, which retrieves the previous row's value. The `LEAD()` function is useful for comparing the current row's value with the value of the next row or performing calculations that involve the subsequent row's data.
The basic syntax of the `LEAD()` function is as follows:
```sql
LEAD(column, offset, default_value) OVER (PARTITION BY partition_expression ORDER BY sort_expression)

```
* `column`: The column from which you want to retrieve the value of the following row.* `offset`: The number of rows to look ahead. An `offset` of 1 indicates the next row, 2 for two rows ahead, and so on.* `default_value`: (Optional) The value returned when the `LEAD()` function reaches the last row or a partition boundary where the offset is beyond the available rows.Let's look at an example to illustrate how the `LEAD()` function works. Consider a table named `sales` with the following data:
```yaml
+------+------------+--------+
| ID   |  Sale_Date  | Amount |
+------+------------+--------+
| 1    | 2023-07-01 | 100    |
| 2    | 2023-07-03 | 150    |
| 3    | 2023-07-02 | 120    |
| 4    | 2023-07-01 | 200    |
| 5    | 2023-07-03 | 180    |
+------+------------+--------+

```
To retrieve the next day's sales amount for each row, we can use the `LEAD()` function:
```sql
SELECT
    ID,
    Sale_Date,
    Amount,
    LEAD(Amount, 1) OVER (ORDER BY Sale_Date) AS Next_Day_Sales
FROM
    sales;

```
Result:
```yaml
+------+------------+--------+-----------------+
| ID   | Sale_Date  | Amount | Next_Day_Sales  |
+------+------------+--------+-----------------+
| 1    | 2023-07-01 | 100    | 200             |
| 4    | 2023-07-01 | 200    | 120             |
| 3    | 2023-07-02 | 120    | 150             |
| 2    | 2023-07-03 | 150    | 180             |
| 5    | 2023-07-03 | 180    | NULL            |
+------+------------+--------+-----------------+

```
In this example, the `LEAD()` function returns the next day's sales amount for each row, ordered by the `Sale_Date`. The last row has a `NULL` value for the `Next_Day_Sales` because there is no following row to reference.
The `LEAD()` function is helpful for performing time-series analysis, calculating differences between consecutive rows, identifying trends, and other types of sequential data processing in SQL. It complements the `LAG()` function and together, they provide a powerful way to analyze data in ordered sequences.


`LISTAGG()` is an aggregate function in SQL used to concatenate values from multiple rows into a single string. This function is especially helpful when you want to combine the values of a specific column for each group into a comma-separated list or any other delimiter-separated list.
The basic syntax of the `LISTAGG()` function is as follows:
```sql
LISTAGG(expression, delimiter) WITHIN GROUP (ORDER BY sort_expression)

```
* `expression`: The column or expression whose values you want to concatenate.* `delimiter`: The character(s) used to separate the values in the resulting string.* `sort_expression`: (Optional) Specifies the order in which the values are concatenated within each group. If not provided, the order is undefined.Let's look at an example to illustrate how the `LISTAGG()` function works. Consider a table named `employees` with the following data:
```diff
+------+----------+-------------+
| ID   |  Name    | Department  |
+------+----------+-------------+
| 1    | Alice    | HR          |
| 2    | Bob      | IT          |
| 3    | Carol    | HR          |
| 4    | David    | IT          |
| 5    | Emma     | HR          |
+------+----------+-------------+

```
To concatenate the names of employees within each department into a comma-separated list, we can use the `LISTAGG()` function:
```sql
SELECT
    Department,
    LISTAGG(Name, ', ') WITHIN GROUP (ORDER BY ID) AS Employee_List
FROM
    employees
GROUP BY
    Department;

```
Result:
```diff
+-------------+---------------------+
| Department  | Employee_List       |
+-------------+---------------------+
| HR          | Alice, Carol, Emma  |
| IT          | Bob, David          |
+-------------+---------------------+

```
In this example, we grouped the employees by their respective departments and used the `LISTAGG()` function to concatenate the names within each department, separated by commas. The order of names within each group is determined by the `ORDER BY` clause in the `LISTAGG()` function (in this case, ordered by the `ID` column).
Please note that the `LISTAGG()` function is available in some database systems like Oracle, but its availability might vary depending on the specific SQL implementation. Additionally, for very long concatenated strings, there may be limitations on the maximum size of the resulting string, depending on the database system.


`ROLLUP()` is an extension to the `GROUP BY` clause in SQL that is used for generating subtotal rows and a grand total row in the result set. It is commonly used to create hierarchical reports that summarize data at different aggregation levels.
The `ROLLUP()` function generates multiple levels of grouping for the specified columns, producing subtotals for each combination of the grouped columns and a grand total row that aggregates all the data.
The basic syntax of the `ROLLUP()` function is as follows:
```sql
SELECT column1, column2, ..., columnN, aggregate_function(columnX)
FROM table_name
GROUP BY ROLLUP(column1, column2, ..., columnN)

```
Here, `column1, column2, ..., columnN` represents the grouping columns, and `aggregate_function(columnX)` is an aggregate function like `SUM()`, `COUNT()`, `AVG()`, etc., applied to the columnX data.
Let's look at an example to better understand the `ROLLUP()` function. Consider a table named `sales` with the following data:
```css
+---------+-----------+--------+
| Product |  Category | Amount |
+---------+-----------+--------+
| A       | Electronics| 100    |
| B       | Fashion   | 150    |
| A       | Electronics| 120    |
| C       | Furniture | 200    |
| B       | Fashion   | 180    |
+---------+-----------+--------+

```
To generate a report that shows subtotals for each product and a grand total:
```sql
SELECT
    Product,
    Category,
    SUM(Amount) AS Total_Amount
FROM
    sales
GROUP BY ROLLUP(Product, Category);

```
Result:
```sql
+---------+-----------+--------------+
| Product |  Category | Total_Amount |
+---------+-----------+--------------+
| A       | Electronics| 220          |
| A       | NULL      | 220          |
| B       | Fashion   | 330          |
| B       | NULL      | 330          |
| C       | Furniture | 200          |
| C       | NULL      | 200          |
| NULL    | NULL      | 750          |
+---------+-----------+--------------+

```
In the result, you can see that `ROLLUP()` created subtotal rows for each product (`A`, `B`, `C`) and a grand total row with `NULL` values for both `Product` and `Category`. The `Total_Amount` column represents the sum of amounts for each combination of the grouped columns.
The `ROLLUP()` function is particularly useful for generating aggregated reports that show different levels of summaries for your data. It simplifies the process of creating hierarchical reports without the need for complex SQL queries or multiple queries.



`CUBE()` is an extension to the `GROUP BY` clause in SQL that is used for generating subtotal rows and a grand total row in the result set, similar to `ROLLUP()`. However, `CUBE()` goes a step further and generates all possible combinations of the specified grouping columns, creating a more extensive set of subtotals and grand totals.
The `CUBE()` function produces a result set that includes subtotals for each combination of the grouped columns, as well as a grand total row that aggregates all the data. It generates a more comprehensive summary of data across multiple dimensions.
The basic syntax of the `CUBE()` function is as follows:
```sql
SELECT column1, column2, ..., columnN, aggregate_function(columnX)
FROM table_name
GROUP BY CUBE(column1, column2, ..., columnN)

```
Here, `column1, column2, ..., columnN` represents the grouping columns, and `aggregate_function(columnX)` is an aggregate function like `SUM()`, `COUNT()`, `AVG()`, etc., applied to the columnX data.
Let's look at an example to better understand the `CUBE()` function. Consider a table named `sales` with the following data:
```css
+---------+-----------+--------+
| Product |  Category | Amount |
+---------+-----------+--------+
| A       | Electronics| 100    |
| B       | Fashion   | 150    |
| A       | Electronics| 120    |
| C       | Furniture | 200    |
| B       | Fashion   | 180    |
+---------+-----------+--------+

```
To generate a report that shows subtotals for each product, category, and their combinations, as well as a grand total:
```sql
SELECT
    Product,
    Category,
    SUM(Amount) AS Total_Amount
FROM
    sales
GROUP BY CUBE(Product, Category);

```
Result:
```sql
+---------+-----------+--------------+
| Product |  Category | Total_Amount |
+---------+-----------+--------------+
| A       | Electronics| 220          |
| A       | NULL      | 220          |
| B       | Fashion   | 330          |
| B       | NULL      | 330          |
| C       | Furniture | 200          |
| C       | NULL      | 200          |
| NULL    | Electronics| 320          |
| NULL    | Fashion   | 330          |
| NULL    | Furniture | 200          |
| NULL    | NULL      | 850          |
+---------+-----------+--------------+

```
In the result, you can see that `CUBE()` created subtotal rows for each combination of `Product` and `Category`, as well as rows that represent subtotals for each individual `Product` and `Category`. The grand total row contains `NULL` values for both `Product` and `Category`, representing the aggregation of all data.
The `CUBE()` function is particularly useful when you need to create comprehensive summary reports across multiple dimensions of your data. It provides a complete view of the data's aggregation at various levels and saves you from writing multiple queries or performing additional data manipulations to achieve the same result.


In Oracle, a Common Table Expression (CTE) is a named temporary result set that you can reference within a SELECT, INSERT, UPDATE, or DELETE statement. CTEs are defined using the `WITH` clause and are particularly useful when you need to break down complex queries into smaller, more manageable parts, making the SQL code more readable and maintainable.
The basic syntax of a CTE in Oracle is as follows:
```sql
WITH cte_name (column1, column2, ..., columnN) AS (
    -- Subquery that defines the CTE
    SELECT column1, column2, ..., columnN
    FROM source_table
    WHERE condition
)
-- Use the CTE in the main query
SELECT *
FROM cte_name;

```
Here's an example to illustrate how to use a CTE in Oracle. Consider a table named `employees` with the following data:
```diff
+------+-----------+--------+
| ID   |  Name     | Salary |
+------+-----------+--------+
| 1    | Alice     | 50000  |
| 2    | Bob       | 60000  |
| 3    | Carol     | 55000  |
| 4    | David     | 58000  |
| 5    | Emma      | 52000  |
+------+-----------+--------+

```
Now, let's use a CTE to find the average salary of employees and select only those employees whose salary is above the average:
```sql
WITH AvgSalary AS (
    SELECT AVG(Salary) AS AverageSalary
    FROM employees
)
SELECT ID, Name, Salary
FROM employees
CROSS JOIN AvgSalary
WHERE Salary > AverageSalary;

```
Result:
```diff
+------+-----------+--------+
| ID   |  Name     | Salary |
+------+-----------+--------+
| 2    | Bob       | 60000  |
| 4    | David     | 58000  |
+------+-----------+--------+

```
In this example, we used a CTE named `AvgSalary` to calculate the average salary of all employees. Then, in the main query, we performed a `CROSS JOIN` with the CTE and selected only those employees whose salary is above the calculated average salary.
CTEs can simplify complex queries, make the SQL code more readable, and reduce redundancy by creating reusable subqueries. They are a powerful tool to optimize and structure your SQL code when dealing with large and intricate data sets.
