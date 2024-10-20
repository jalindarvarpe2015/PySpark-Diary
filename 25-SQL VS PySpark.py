# SQL Vs PySpark

# SQL is great for structured queries with defined schema and is easy to use with relational databases.

# PySpark is more flexible, suited for big data processing, and allows working on distributed data in a highly scalable way.


1- Data Retrival : SQL queries are used to retrieve data from relational databases, while PySpark DataFrames can be used to retrieve data from HDFS, S3, and other distributed file systems.

2- Data Manipulation : SQL requires more complex syntax for data manipulation, while PySpark DataFrames provide a more user-friendly API for performing various operations on data.

3- Scalability : PySpark DataFrames can handle large datasets distributed across multiple nodes, while SQL requires a single server to handle the data.

4- Performance : PySpark DataFrames are optimized for performance, while SQL may not be as efficient for complex queries.

5- Flexibility : PySpark DataFrames provide more flexibility in terms of data types, while SQL is more strict with data types.

6- Integration : PySpark DataFrames can integrate with other Python libraries, while SQL may require additional code to integrate with other languages.

7- Security : PySpark DataFrames can be secured with encryption, while SQL requires a separate authentication mechanism.

8- Support : PySpark DataFrames have better support for various data processing tasks, while SQL may not have as many features.

9- Learning curve : PySpark DataFrames have a steeper learning curve, while SQL may be easier to learn.

10- Performance : PySpark DataFrames are optimized for performance, while SQL may not be as efficient for complex queries.

# SQL

select * from employee where age>30;

df1 = df.filter(df.age>30)

2 - Data Cleaning : 
# SQL provides a wide range of functions for data cleaning, such as handling missing values, removing duplicates, and filtering data based on conditions.

select * from employee where name is not null;

df1 = df.filter(df.name.isNotNull())

3-sorting : 

select * from employee order by salary desc;

df1 = df.orderBy(df.salary.desc())

4 - Aggregation :

SELECT department, COUNT(*) FROM employees GROUP BY department;

df1 = df.groupBy(df.department).count()

5.Joins :

SELECT * FROM employees e JOIN departments d ON e.department_id = d.id;

df1 = df.join(df1, df.department_id == df1.id)

6.Limit Rows :

select * from employee limit 10;

df1 = df.limit(10).show()

7.Null Handling :

select * from employee where name is null;

df1 = df.filter(df.name.isNull())

8.Updating Records :

update employees set salary = 100000 where id = 1;
UPDATE employees SET salary = salary * 1.1 WHERE department = 'HR';


df = df.withColumn("salary",when(df["department"] == 'HR',df["salary"] * 1.1).otherwise(df["salary"]))
df1 = df.withColumn("salary", when(df.id == 1, 100000).otherwise(df.salary))

9.Window Functions :

select department, avg(salary) over (partition by department) as avg_salary from employees;
SELECT name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank FROM employees;

df1 = df.select(df.department, F.avg(df.salary).over(Window.partitionBy(df.department)).alias("avg_salary"))
windowSpec = Window.partitionBy("department").orderBy(df["salary"].desc()) df.withColumn("rank", rank().over(windowSpec)).show()

9.Subqueries :

select * from employees where salary > (select avg(salary) from employees);


# PySpark:PySpark doesn't support subqueries directly in the same manner as SQL. You need to calculate the result separately and then apply the filter:

df1 = df.filter(df.salary > df.select(F.avg(df.salary)).collect()[0][0])

avg_salary = df.agg({"salary":"avg"}).collect()[0]["avg(salary)"]df.filter(df["salary"] > avg_salary).show()


10.Insert Data  :

insert into employees (name, department, salary) values ('John', 'IT', 60000);

# PySpark DataFrames are immutable, so to "insert" new rows, you create a new DataFrame and union it

new_data = [("John", 28, "HR")]
df_new = spark.createDataFrame(new_data,["name", "age", "department"])
df = df.union(df_new)
df.show()

11.Deleting Records :

delete from employees where department = 'HR';


PySpark: PySpark doesn't allow deletion directly. You would filter out the unwanted rows:

df = df.filter(df["age"] >= 18)
df.show()

df1 = df.filter(~(df.department == 'HR'))


12.Distinct Values  :

select distinct department from employees;

df1 = df.select(df.department).distinct()

13.Aggregation with Multiple Functions  :

select department, count(*), avg(salary) from employees group by department;

df1 = df.groupBy(df.department).agg(F.count("*").alias("count"), F.avg(df.salary).alias("avg_salary"))
