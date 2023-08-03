**SQL for Data engineering:**

SQL (Structured Query Language) is a powerful tool for data engineering tasks, enabling you to manipulate, transform, and manage data efficiently. Below, I'll provide some common SQL operations and tasks often used in data engineering workflows:


These are some essential SQL operations for data engineering tasks. Keep in mind that the actual SQL code you use may vary depending on the specific database system you are working with (e.g., MySQL, PostgreSQL, SQL Server, etc.). Always refer to the documentation of the database system you are using for more detailed information.

**RANK()**

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

**DENSE_RANK():**
   The `DENSE_RANK()` function is used to assign a unique rank to each distinct row within a result set. If two or more rows have the same values for the sorting criteria, they will be assigned the same rank, and the next rank(s) will be skipped. The ranks are dense, meaning there are no gaps in the ranking sequence.

   Syntax:
   ```sql
   DENSE_RANK() OVER (PARTITION BY partition_expression ORDER BY sort_expression [ASC|DESC])
   ```

   Example:
   Let's use the same `students` table as before and apply `DENSE_RANK()`:

   ```sql
   SELECT 
       ID,
       Name,
       Score,
       DENSE_RANK() OVER (ORDER BY Score DESC) AS Dense_Ranking
   FROM
       students;
   ```

   Result:
   ```
   +------+-------+-------+---------------+
   | ID   | Name  | Score | Dense_Ranking |
   +------+-------+-------+---------------+
   | 2    | Bob   | 90    | 1             |
   | 4    | David | 90    | 1             |
   | 1    | Alice | 85    | 2             |
   | 5    | Emma  | 82    | 3             |
   | 3    | Carol | 78    | 4             |
   +------+-------+-------+---------------+
   ```

**ROW_NUMBER():**
   The `ROW_NUMBER()` function assigns a unique sequential number to each row within a result set, regardless of any ties in the sorting criteria. This means that every row receives a different number, even if multiple rows have the same values for the sorting column(s).

   Syntax:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY partition_expression ORDER BY sort_expression [ASC|DESC])
   ```

   Example:
   Using the `students` table:

   ```sql
   SELECT 
       ID,
       Name,
       Score,
       ROW_NUMBER() OVER (ORDER BY Score DESC) AS Row_Num
   FROM
       students;
   ```

   Result:
   ```
   +------+-------+-------+--------+
   | ID   | Name  | Score | Row_Num|
   +------+-------+-------+--------+
   | 2    | Bob   | 90    | 1      |
   | 4    | David | 90    | 2      |
   | 1    | Alice | 85    | 3      |
   | 5    | Emma  | 82    | 4      |
   | 3    | Carol | 78    | 5      |
   +------+-------+-------+--------+
   ```

**NTILE():**
   The `NTILE()` function divides the result set into a specified number of equally sized buckets or groups and assigns a group number to each row. If there are N rows and the `NTILE(N)` function is used, each group will contain approximately the same number of rows. It's particularly useful when you need to divide data into quartiles, deciles, or other quantiles.

   Syntax:
   ```sql
   NTILE(num_buckets) OVER (PARTITION BY partition_expression ORDER BY sort_expression [ASC|DESC])
   ```

   Example:
   Using the `students` table, let's divide the data into 3 groups:

   ```sql
   SELECT 
       ID,
       Name,
       Score,
       NTILE(3) OVER (ORDER BY Score DESC) AS Bucket
   FROM
       students;
   ```

   Result:
   ```
   +------+-------+-------+--------+
   | ID   | Name  | Score | Bucket |
   +------+-------+-------+--------+
   | 2    | Bob   | 90    | 1      |
   | 4    | David | 90    | 1      |
   | 1    | Alice | 85    | 2      |
   | 5    | Emma  | 82    | 2      |
   | 3    | Carol | 78    | 3      |
   +------+-------+-------+--------+
   ```

As you can see, the `NTILE(3)` function divided the data into three groups, each containing two students.

**COUNT():**
   The `COUNT()` function returns the number of rows in the current window (partition). It can be used to count the occurrences of a particular column or to count all rows in the partition.

   Syntax:
   ```sql
   COUNT(expression) OVER (PARTITION BY partition_expression ORDER BY sort_expression)
   ```

   Example:
   Suppose we have a table named `orders` with the following data:

   ```
   +------+--------+-------+
   | ID   |  Name  | Price |
   +------+--------+-------+
   | 1    | Item1  | 10    |
   | 2    | Item2  | 15    |
   | 3    | Item1  | 20    |
   | 4    | Item3  | 12    |
   | 5    | Item2  | 18    |
   +------+--------+-------+
   ```

   To get the count of orders for each item:

   ```sql
   SELECT
       ID,
       Name,
       Price,
       COUNT(*) OVER (PARTITION BY Name) AS Item_Count
   FROM
       orders;
   ```

   Result:
   ```
   +------+--------+-------+------------+
   | ID   |  Name  | Price | Item_Count |
   +------+--------+-------+------------+
   | 1    | Item1  | 10    | 2          |
   | 2    | Item2  | 15    | 2          |
   | 3    | Item1  | 20    | 2          |
   | 4    | Item3  | 12    | 1          |
   | 5    | Item2  | 18    | 2          |
   +------+--------+-------+------------+

**AVG():**
   The `AVG()` function calculates the average value of a column within the current window (partition). It is useful for finding the average of numerical values.

   Syntax:
   ```sql
   AVG(expression) OVER (PARTITION BY partition_expression ORDER BY sort_expression)
   ```

   Example:
   Continuing with the `orders` table, to find the average price for each item:

   ```sql
   SELECT
       ID,
       Name,
       Price,
       AVG(Price) OVER (PARTITION BY Name) AS Avg_Price
   FROM
       orders;
   ```

   Result:
   ```
   +------+--------+-------+-----------+
   | ID   |  Name  | Price | Avg_Price |
   +------+--------+-------+-----------+
   | 1    | Item1  | 10    | 15        |
   | 2    | Item2  | 15    | 16.5      |
   | 3    | Item1  | 20    | 15        |
   | 4    | Item3  | 12    | 12        |
   | 5    | Item2  | 18    | 16.5      |
   +------+--------+-------+-----------+

**SUM():**
   The `SUM()` function calculates the total sum of a column within the current window (partition). It is used for calculating the total of numerical values.

   Syntax:
   ```sql
   SUM(expression) OVER (PARTITION BY partition_expression ORDER BY sort_expression)
   ```

   Example:
   Continuing with the `orders` table, to find the total sales (sum of prices) for each item:

   ```sql
   SELECT
       ID,
       Name,
       Price,
       SUM(Price) OVER (PARTITION BY Name) AS Total_Sales
   FROM
       orders;
   ```

   Result:
   ```
   +------+--------+-------+-------------+
   | ID   |  Name  | Price | Total_Sales |
   +------+--------+-------+-------------+
   | 1    | Item1  | 10    | 30          |
   | 2    | Item2  | 15    | 33          |
   | 3    | Item1  | 20    | 30          |
   | 4    | Item3  | 12    | 12          |
   | 5    | Item2  | 18    | 33          |
   +------+--------+-------+-------------+

These window functions (`COUNT()`, `AVG()`, and `SUM()`) are powerful tools for performing analytical calculations over specific partitions of data within a result set, providing valuable insights into your data.

**FIRST_VALUE():**
   The `FIRST_VALUE()` function returns the value of a specified expression from the first row within the current window (partition) based on the specified ordering. It helps you extract the first occurrence of a column value within each group.

   Syntax:
   ```sql
   FIRST_VALUE(expression) OVER (PARTITION BY partition_expression ORDER BY sort_expression)
   ```

   Example:
   Suppose we have a table named `sales` with the following data:

   ```
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

   To get the first sale amount for each sale date:

   ```sql
   SELECT
       ID,
       Sale_Date,
       Amount,
       FIRST_VALUE(Amount) OVER (PARTITION BY Sale_Date ORDER BY ID) AS First_Sale_Amount
   FROM
       sales;
   ```

   Result:
   ```
   +------+------------+--------+-----------------+
   | ID   | Sale_Date  | Amount | First_Sale_Amount|
   +------+------------+--------+-----------------+
   | 1    | 2023-07-01 | 100    | 100             |
   | 2    | 2023-07-03 | 150    | 150             |
   | 3    | 2023-07-02 | 120    | 120             |
   | 4    | 2023-07-01 | 200    | 100             |
   | 5    | 2023-07-03 | 180    | 150             |
   +------+------------+--------+-----------------+

**LAST_VALUE():**
   The `LAST_VALUE()` function returns the value of a specified expression from the last row within the current window (partition) based on the specified ordering. It helps you extract the last occurrence of a column value within each group.

   Syntax:
   ```sql
   LAST_VALUE(expression) OVER (PARTITION BY partition_expression ORDER BY sort_expression)
   ```

   Example:
   Continuing with the `sales` table, to get the last sale amount for each sale date:

   ```sql
   SELECT
       ID,
       Sale_Date,
       Amount,
       LAST_VALUE(Amount) OVER (PARTITION BY Sale_Date ORDER BY ID) AS Last_Sale_Amount
   FROM
       sales;
   ```

   Result:
   ```
   +------+------------+--------+----------------+
   | ID   | Sale_Date  | Amount | Last_Sale_Amount|
   +------+------------+--------+----------------+
   | 1    | 2023-07-01 | 100    | 200            |
   | 2    | 2023-07-03 | 150    | 180            |
   | 3    | 2023-07-02 | 120    | 120            |
   | 4    | 2023-07-01 | 200    | 200            |
   | 5    | 2023-07-03 | 180    | 180            |
   +------+------------+--------+----------------+
   
Note: The `LAST_VALUE()` function may not always provide the expected results in all SQL database systems. This is due to the way data is ordered and processed in some systems. To ensure accurate results, consider using a subquery or a self-join to get the last value instead.
**LAG()**
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

**LEAD()**
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

**LISTAGG()**
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

**ROLLUP()**
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

**CUBE()**
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

**Common Table Expression**
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

**PL/SQL**
In PL/SQL, which is Oracle's procedural extension of SQL, you can use several types of collections for data manipulation, including those applicable to data science. Collections are composite data types that allow you to store and process multiple values as a single unit. Some commonly used collections in PL/SQL for data science purposes include:


When working with data science in PL/SQL, you can use these collection types to hold and manipulate data, perform calculations, and implement algorithms. They provide flexibility and efficiency in handling large datasets within PL/SQL procedures and functions.
Remember that while PL/SQL can be useful for data manipulation and some data analysis tasks, data science tasks typically require more specialized tools and libraries, such as Python with libraries like Pandas, NumPy, and Scikit-learn. If you are involved in data science work, using Python or other specialized data science tools is likely to be more suitable for most data analysis and machine learning tasks.

Bulk processing and context switching are two concepts used in database management and programming to optimize the performance of data operations and reduce the overhead associated with repetitive tasks. Let's describe each concept with examples and explore their use cases:


**Combining Bulk Processing and Context Switching:**
To optimize data processing, it's essential to minimize context switching and leverage bulk processing where possible. This can be achieved by performing data operations using set-based SQL operations rather than row-by-row processing in procedural code.
```sql
-- Bulk Processing using a single SQL UPDATE statement
UPDATE employees
SET salary = salary * 1.1
WHERE department = 'HR';

```
By using the above approach, we avoid context switches during repetitive operations and significantly improve performance, especially when dealing with large datasets.
In summary, bulk processing and minimizing context switching are crucial techniques in data management and programming to achieve efficient data operations and improve overall performance. By adopting these practices, you can optimize data manipulation tasks and reduce unnecessary overhead in your applications and database operations.


In the context of programming and database management, context switching carries a performance cost due to the overhead involved in switching execution from one context (e.g., application code) to another context (e.g., database engine or operating system). Context switching is a fundamental process in multitasking operating systems, and it allows multiple processes or threads to share a single CPU core efficiently. However, it is not without its performance implications.
The performance impact of context switching can vary depending on several factors, including the operating system, hardware, the frequency of context switches, and the nature of the tasks being performed. Here are some key points to consider regarding the performance impact of context switching:


To mitigate the performance impact of context switching, it's essential to design applications and algorithms in a way that minimizes unnecessary context switches. Some strategies to improve performance include:
* Using bulk processing and set-based operations whenever possible to reduce the number of individual context switches.* Optimizing algorithms to minimize the need for frequent task switches during computations.* Using asynchronous programming and event-driven architectures to handle I/O operations efficiently and avoid blocking and unnecessary context switches.* Implementing thread pooling and other thread management techniques to reduce thread creation and destruction overhead.In summary, while context switching introduces performance overhead, it is a necessary mechanism for multitasking systems to achieve concurrency and parallelism. Careful design and optimization of applications and algorithms can help mitigate the impact of context switching on overall system performance.


**BULK COLLECT**

`BULK COLLECT` is a powerful feature in PL/SQL that allows you to retrieve multiple rows of data from the database into collections in a single operation. It is designed to improve the performance of data retrieval and processing by reducing the number of individual context switches between the database and the application.
The basic syntax of the `BULK COLLECT` statement is as follows:
```sql
BULK COLLECT INTO collection_name
(SELECT column1, column2, ..., columnN FROM table_name WHERE condition);

```
Here, `collection_name` is the name of the PL/SQL collection (e.g., associative array, nested table, or VARRAY) that will store the retrieved data. The `SELECT` statement inside the parentheses is the query that retrieves the data from the database.
Let's look at an example to illustrate how `BULK COLLECT` works:
```sql
DECLARE
    TYPE EmpNameArray IS TABLE OF employees.first_name%TYPE;
    emp_names EmpNameArray;
BEGIN
    SELECT first_name
    BULK COLLECT INTO emp_names
    FROM employees
    WHERE department = 'HR';

    FOR i IN 1..emp_names.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(emp_names(i));
    END LOOP;
END;

```
In this example, we declare a collection `EmpNameArray`, which is an array of employee first names. The `BULK COLLECT` statement retrieves all first names from the employees table where the department is 'HR' and stores them in the `emp_names` collection. Then, we loop through the collection and print the employee names using `DBMS_OUTPUT`.
Advantages of `BULK COLLECT`:


`BULK COLLECT` is especially useful when dealing with large datasets or when you need to retrieve multiple columns or rows of data for further processing. It is widely used in PL/SQL to enhance the performance of data retrieval and processing operations, making it an essential feature for efficient database programming.

**FORALL**
`FORALL` is another powerful feature in PL/SQL that is used in conjunction with collections to perform bulk data manipulation efficiently. It allows you to execute a DML (Data Manipulation Language) statement (e.g., INSERT, UPDATE, DELETE) for multiple rows in a single operation, instead of processing each row individually. This can significantly improve the performance of data modification operations.
The basic syntax of the `FORALL` statement is as follows:
```sql
FORALL index IN lower_bound..upper_bound
    DML_statement;

```
Here, `index` is a loop index variable that iterates from `lower_bound` to `upper_bound`. The `DML_statement` is the DML operation (e.g., INSERT, UPDATE, DELETE) that you want to perform for each element in the collection. The `FORALL` statement will execute the DML statement for all elements in the collection in a single batch.
Let's look at an example to illustrate how `FORALL` works:
```sql
DECLARE
    TYPE EmpIDArray IS TABLE OF employees.employee_id%TYPE;
    emp_ids EmpIDArray := EmpIDArray(101, 102, 103, 104, 105);
BEGIN
    FORALL i IN 1..emp_ids.COUNT
        UPDATE employees
        SET salary = salary * 1.1
        WHERE employee_id = emp_ids(i);
END;

```
In this example, we declare a collection `EmpIDArray`, which is an array of employee IDs. The `FORALL` statement updates the salary for each employee in the `emp_ids` collection in a single batch, increasing their salaries by 10%.
Advantages of `FORALL`:


`FORALL` is especially beneficial when performing data modifications on a large number of rows or when you need to update, insert, or delete data for multiple records in a single operation. It is a valuable tool for improving the performance of data manipulation tasks in PL/SQL programs.

**Materialised view**

A Materialized View (MV) is a database object in SQL that stores the results of a query in a physical table-like structure. Unlike regular views, which are virtual and do not store data, materialized views actually hold the data resulting from a specific query. Materialized views are especially useful for improving query performance by precomputing and storing the results of complex or frequently used queries.
When data in the underlying tables changes, the materialized view can be refreshed to ensure that it reflects the most up-to-date data. There are different ways to refresh materialized views, such as on-demand or based on a schedule, depending on the database system and configuration.
The benefits of using materialized views include:


However, it's essential to consider the trade-offs while using materialized views:

In summary, materialized views are a valuable tool for improving query performance and reducing workload on source tables in databases. They are particularly useful for scenarios where complex queries are run frequently, and where the latest data is not required for every query. However, proper planning and consideration of the trade-offs are essential to make the best use of materialized views in a database system.

Let's demonstrate the use of a materialized view with an example. In this example, we'll create a materialized view that stores the total sales amount for each product category from an `orders` table.
Consider the following `orders` table:
```diff
+--------+------------+-------------+
| OrderID| Product    | SalesAmount |
+--------+------------+-------------+
| 1      | Laptop     | 1200        |
| 2      | Smartphone | 800         |
| 3      | Laptop     | 1400        |
| 4      | Tablet     | 600         |
| 5      | Smartphone | 900         |
+--------+------------+-------------+

```
We want to create a materialized view to store the total sales amount for each product category.

**Create Materialized View:**

```sql
CREATE MATERIALIZED VIEW mv_product_sales AS
SELECT Product, SUM(SalesAmount) AS TotalSales
FROM orders
GROUP BY Product;

```
The above SQL statement creates a materialized view named `mv_product_sales`. It calculates the total sales amount for each product category from the `orders` table and stores the results in the materialized view.

**Query Materialized View:**

```sql
SELECT * FROM mv_product_sales;

```
The query to the materialized view will provide the following result:
```diff
+------------+-------------+
| Product    | TotalSales  |
+------------+-------------+
| Laptop     | 2600        |
| Smartphone | 1700        |
| Tablet     | 600         |
+------------+-------------+

```
The materialized view `mv_product_sales` contains the precomputed total sales amount for each product category, providing quick access to aggregated data without having to recompute the sum of sales amounts each time the query is executed.

**Refresh Materialized View:**

```sql
BEGIN
    DBMS_MVIEW.REFRESH('mv_product_sales');
END;

```
To keep the materialized view up-to-date, we can refresh it on-demand or on a scheduled basis. The above PL/SQL block demonstrates refreshing the materialized view manually.
With this materialized view, we can significantly improve query performance when retrieving the total sales amount for each product category. Instead of repeatedly calculating the sums from the `orders` table, the database will directly access the precomputed results stored in the materialized view.
Please note that the syntax and usage of materialized views may vary depending on the specific database management system (e.g., Oracle, PostgreSQL, etc.) being used. The example provided above uses Oracle syntax.

The "build immediate" clause in the context of creating a materialized view is used to indicate that the materialized view should be populated with data immediately upon creation. It ensures that the materialized view is fully refreshed and up-to-date before the creation process completes. This is useful when you want to ensure that the materialized view is ready to be queried as soon as it is created.
Let's revisit the example of creating a materialized view for the total sales amount for each product category, but this time we'll include the "build immediate" clause:
```sql
CREATE MATERIALIZED VIEW mv_product_sales 
BUILD IMMEDIATE
AS
SELECT Product, SUM(SalesAmount) AS TotalSales
FROM orders
GROUP BY Product;

```
With the "build immediate" clause, the materialized view `mv_product_sales` will be populated with data as soon as the materialized view creation statement is executed. The database will perform the necessary computations and populate the materialized view with the aggregated sales data based on the query specified.
Please note that the "build immediate" clause is optional, and if not specified, the materialized view is created with an "on demand" build option. In that case, the materialized view will be populated with data during the first refresh operation, which can be done manually or based on a defined refresh schedule.
When using "build immediate," the materialized view will be available for querying immediately after creation. However, depending on the size of the underlying data and complexity of the query, the initial population of the materialized view may take some time.
In summary, the "build immediate" clause is used to ensure that a materialized view is populated with data immediately upon creation, allowing it to be queried without delay. This can be useful in scenarios where you want to have a readily available materialized view for quick data access.

When you create a materialized view with the `BUILD DEFERRED` clause, it means that the materialized view is initially empty, and its data is not populated immediately during the creation process. Instead, the materialized view will be populated with data later when a specific refresh operation is explicitly performed.
Let's take the example of creating a materialized view for the total sales amount for each product category with the `BUILD DEFERRED` clause:
```sql
CREATE MATERIALIZED VIEW mv_product_sales
BUILD DEFERRED
AS
SELECT Product, SUM(SalesAmount) AS TotalSales
FROM orders
GROUP BY Product;

```
With the `BUILD DEFERRED` clause, the materialized view `mv_product_sales` will not contain any data after the creation process. The data will remain empty until you manually refresh the materialized view using one of the following methods:


```sql
BEGIN
    DBMS_MVIEW.REFRESH('mv_product_sales');
END;

```

```sql
CREATE OR REPLACE JOB refresh_mv_product_sales
    INTERVAL '1' HOUR
    NEXT_DATE SYSDATE
    AS
BEGIN
    DBMS_MVIEW.REFRESH('mv_product_sales');
END;

```
**BUILD DEFERRED**
Using `BUILD DEFERRED` gives you the flexibility to control when the data is populated in the materialized view. This can be advantageous in situations where you have large datasets, and you want to manage the timing of the initial population and subsequent refreshes to optimize system resources and performance.
Please keep in mind that the specific syntax and behavior of materialized views, including the use of `BUILD DEFERRED`, may vary depending on the database system being used (e.g., Oracle, PostgreSQL, etc.). Always refer to the documentation of your specific database management system for detailed information on creating and managing materialized views.

In the context of materialized views, there are several refresh types available that determine how the materialized view's data is updated or refreshed to reflect changes in the underlying base tables. Each refresh type offers a different approach to keeping the materialized view up-to-date with the latest data. The common refresh types are:


The choice of refresh type depends on factors such as the frequency of data changes in the base tables, the size of the materialized view, the desired level of freshness of the data, and the system resources available. Careful consideration of the refresh type can help optimize the performance and usability of materialized views in database applications.

Both on-demand and automatic refreshes have their use cases, and the choice depends on your specific requirements and the data update patterns in the underlying base tables. On-demand refresh gives you more control over when the refresh occurs, while automatic refresh takes care of refreshing the materialized view based on a predefined schedule.

**ON COMMIT**
"ON COMMIT" is another way to refresh a materialized view, but it is specifically used in the context of maintaining a materialized view's data automatically and immediately after each data change in the underlying base tables.
When you create a materialized view with the "ON COMMIT" clause, it specifies that the materialized view is to be refreshed automatically and immediately after each transaction that modifies the data in the base tables. The refresh happens as part of the same transaction that performs the data modification.
Let's look at an example of creating a materialized view with the "ON COMMIT" clause:
```sql
CREATE MATERIALIZED VIEW mv_product_sales
ON COMMIT REFRESH;
AS
SELECT Product, SUM(SalesAmount) AS TotalSales
FROM orders
GROUP BY Product;

```
**ON COMMIT REFRESH**
With "ON COMMIT REFRESH," the materialized view `mv_product_sales` will be refreshed automatically and immediately after each transaction that modifies data in the `orders` table. If there are multiple changes within a transaction, the materialized view is updated once after the entire transaction is completed successfully.
Using "ON COMMIT" refresh is particularly useful when you need the materialized view to always reflect the latest data in real-time, immediately after changes occur in the base tables. It is commonly employed in scenarios where you have frequently updated data, and you want the materialized view to provide the latest aggregated or precomputed information without any delay.
Please note that "ON COMMIT" refresh is available in some database management systems, such as Oracle. The exact syntax and behavior may vary depending on the database system being used. Additionally, "ON COMMIT" refresh may have some performance considerations as it refreshes the materialized view as part of the transaction, which can impact the overall transaction processing time. Careful evaluation and testing should be done based on the specific use case and data update patterns to determine if "ON COMMIT" refresh is suitable for your application.


**Partitioning**

Partitioning is a database design technique used to improve the manageability, performance, and availability of large tables or indexes by dividing them into smaller, more manageable units called partitions. Each partition acts as a separate logical storage unit within the table or index, and data is distributed across these partitions based on a defined partitioning key. Partitioning can significantly enhance query performance, reduce maintenance overhead, and provide better scalability for large datasets.
Let's explore the concept of partitioning with examples:
<p>**Example Table: Sales**
Consider a table named "Sales" that stores transactional data for a retail company. The table contains various columns, including transaction date, product ID, customer ID, and sales amount.</p>```sql
CREATE TABLE Sales (
    transaction_id NUMBER,
    transaction_date DATE,
    product_id NUMBER,
    customer_id NUMBER,
    sales_amount NUMBER
);

```
**Scenario with Partitioning:**
To address the challenges posed by large tables, we can use partitioning based on a partitioning key. In this example, we'll partition the "Sales" table based on the transaction date.</p>```sql
CREATE TABLE Sales (
    transaction_id NUMBER,
    transaction_date DATE,
    product_id NUMBER,
    customer_id NUMBER,
    sales_amount NUMBER
)
PARTITION BY RANGE (transaction_date)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(
    PARTITION p1 VALUES LESS THAN (TO_DATE('2023-01-01', 'YYYY-MM-DD'))
);

```
In this example, we are using "RANGE" partitioning based on the "transaction_date" column. The "INTERVAL" clause defines the partition interval, which automatically creates new partitions as new data is inserted, based on the specified interval (e.g., 1 day).
With partitioning, the "Sales" table is divided into multiple partitions based on the range of "transaction_date" values. For example:
* Partition P1: Contains data with "transaction_date" values less than '2023-01-01'.* Partition P2: Automatically created and contains data with "transaction_date" values between '2023-01-01' and '2023-01-02'.* Partition P3: Automatically created and contains data with "transaction_date" values between '2023-01-02' and '2023-01-03'.* And so on...**Benefits of Partitioning:**

1. **Query Performance:** Partition pruning allows the database to skip irrelevant partitions when executing queries, resulting in faster and more efficient data retrieval.
2. **Maintenance Flexibility:** Partitioning enables the management of data at a partition level, allowing for easier and faster data maintenance operations like archiving or purging old data.
3. **Reduced I/O and Locking:** Smaller partitions mean less data needs to be scanned and locked during queries, which reduces contention and improves concurrency.
4. **Scalability:** Partitioning provides better scalability, as data is distributed across multiple partitions, allowing for better parallel processing capabilities.
5. **High Availability:** In the event of a hardware failure, the unaffected partitions remain accessible, ensuring high availability for critical data.

Partitioning is a powerful feature in Oracle (and other databases) that can significantly improve performance and manageability for large tables with high volumes of data. However, proper design and understanding of the data access patterns are crucial to fully leverage the benefits of partitioning.


In Oracle, there are several types of partitioning techniques that you can use to organize data in large tables or indexes. Each type of partitioning offers different ways to distribute and manage data based on the partitioning key. Here are the common types of partitioning in Oracle:


Each type of partitioning has its advantages and is suited for different data distribution and access patterns. The choice of partitioning method depends on factors such as data volume, data distribution, and query requirements. Properly designed partitioning can significantly improve performance and manageability for large databases with large datasets.

**Auto List Partitioning:**

Auto List Partitioning is a feature introduced in Oracle Database 12c and later versions. It provides an automatic way to create partitions based on unique values in a specified column. When new unique values are encountered in the partitioning key column during data insertion, Oracle automatically creates new partitions to accommodate these values.
<p>Example:
Let's consider a table for storing sales data with the "product_category" as the partitioning key:</p>```sql
CREATE TABLE Sales (
    transaction_id NUMBER,
    transaction_date DATE,
    product_id NUMBER,
    product_category VARCHAR2(50)
)
PARTITION BY LIST (product_category)
AUTO (DEFAULT)
(
    PARTITION p_default VALUES (DEFAULT)
);

```
In this example, we use the `AUTO (DEFAULT)` clause to enable auto list partitioning. The `DEFAULT` partition will hold any data that does not match any specific partition.
When you insert data into the "Sales" table with new "product_category" values, Oracle automatically creates new partitions as needed. For example:
```sql
INSERT INTO Sales VALUES (101, TO_DATE('2023-07-25', 'YYYY-MM-DD'), 1001, 'Electronics');

```
If "Electronics" is a new product category not existing in any partition, Oracle automatically creates a new partition for it.
Auto List Partitioning is particularly useful when you have rapidly growing datasets with varying partition keys and want to automate the partition management process.

**Virtual Column Based Partitioning:**

Virtual Column Based Partitioning allows you to define virtual columns as the partitioning key, enabling you to use complex expressions or functions to determine partition boundaries. Virtual columns are not physically stored in the table but are computed on-the-fly when queried or used as partitioning keys.
<p>Example:
Suppose you have a "Sales" table with a virtual column "sales_month" to extract the month from the "transaction_date" column for partitioning:</p>```sql
CREATE TABLE Sales (
    transaction_id NUMBER,
    transaction_date DATE,
    product_id NUMBER,
    sales_amount NUMBER,
    sales_month GENERATED ALWAYS AS (EXTRACT(MONTH FROM transaction_date))
)
PARTITION BY RANGE (sales_month)
(
    PARTITION p1 VALUES LESS THAN (4),
    PARTITION p2 VALUES LESS THAN (7),
    PARTITION p3 VALUES LESS THAN (10),
    PARTITION p4 VALUES LESS THAN (MAXVALUE)
);

```
In this example, we use the `GENERATED ALWAYS AS` clause to create the virtual column "sales_month," which extracts the month from the "transaction_date" column. The "Sales" table is then partitioned using the "sales_month" virtual column with range partitioning.
The use of virtual columns as partitioning keys allows for more dynamic partitioning strategies, as the partition boundaries are determined based on the computed values of the virtual columns.
Virtual Column Based Partitioning is valuable when you need more complex partitioning rules that involve functions or expressions on the data. It provides greater flexibility and reduces the need for additional storage to accommodate the virtual columns.
Both Auto List Partitioning and Virtual Column Based Partitioning offer advanced partitioning options that can improve data organization, performance, and management in large databases with diverse data patterns. When designing partitioning strategies, consider the specific requirements of your application and the characteristics of your data to choose the most suitable partitioning technique.
