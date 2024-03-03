# What is Split ?
# PySpark function that splits a single column into multiple column based on certain logic

'''
pyspark.sql.functions.split(str, pattern, limit=-1)
'''
# str = a string expression to split
# pattern = a string representing a regular expression
# limit = optional; an integer that controls the number of times pattern is applied


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

employee_data = [
    (1, 'John Doe', '2021-01-15', 'HR', 60000),
    (2, 'Jane Smith', '2022-03-20', 'IT', 70000),
    (3, 'Bob Johnson', '2020-11-10', 'Finance', 80000),
    (4, 'Alice Brown', '2023-02-05', 'Marketing', 75000),
    (5, 'Charlie White', '2021-09-30', 'Sales', 90000)
]

employee_schema = ('employee_id','name','doj','dept','salary')

empDF = spark.createDataFrame(data=employee_data, schema = employee_schema)

empDF.show()

from pyspark.sql.functions import split

df1 = empDF.withColumn('first_name',split(empDF['name'],' ').getItem(0))\
    .withColumn('last_name', split(empDF['name'],' ').getItem(1))
df1.show()



# Second method to split 

import pyspark

split_col = pyspark.sql.functions.split(empDF['name'],' ')


df2 = empDF.withColumn('First_name',split_col.getItem(0))\
    .withColumn('last_name',split_col.getItem(1))

df2.show()


# Third method to split 

split_col = pyspark.sql.functions.split(empDF['doj'],'-')

df3 = empDF.select('employee_id','name','doj','dept','salary',
                   split_col.getItem(0).alias('joining_year'),split_col.getItem(1).alias('joining_month'),split_col.getItem(2).alias('joining_day'))

df3.show()

# Combine Multiple Split 

df4 = empDF.withColumn('firs_name',split(empDF['name'],' ').getItem(0))\
    .withColumn('last_name',split(empDF['name'],' ').getItem(1))\
        .withColumn('joining_year',split(empDF['doj'],'-').getItem(0))\
            .withColumn('joining_month',split(empDF['doj'],'-').getItem(1))\
                .withColumn('joining_day',split(empDF['doj'],'-').getItem(2))\
        
df4.show()



# Split and drop splitted columns

df5 = empDF.withColumn('firs_name',split(empDF['name'],' ').getItem(0))\
    .withColumn('last_name',split(empDF['name'],' ').getItem(1))\
    .withColumn('joining_year',split(empDF['doj'],'-').getItem(0))\
    .withColumn('joining_month',split(empDF['doj'],'-').getItem(1))\
    .withColumn('joining_day',split(empDF['doj'],'-').getItem(2))\
    .drop(empDF['name'])\
    .drop(empDF['doj'])

df5.show()




