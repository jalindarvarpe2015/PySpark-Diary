# Pivot - It is used to transpose the list of values of a column into column

# Unpivot - It is quite opposite to pivot i.e transposing into list of values to a column

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

data =  [
    ("ABC", "Q1", 2000),
    ("ABC", "Q2", 3000),
    ("ABC", "Q3", 6000),
    ("ABC", "Q4", 1000),
    ("XYZ", "Q1", 1200),
    ("XYZ", "Q2", 4000),
    ("XYZ", "Q3", 4356),
    ("XYZ", "Q4", 6000),
    ("KLM", "Q1", 3456),
    ("KLM", "Q2", 3453),
    ("KLM", "Q3", 3456),
    ("KLM", "Q4", 3899),   
]

column = ['Company','Quarter','Revenue']
 
df = spark.createDataFrame(data = data,schema = column)
df.show()

# Pivot DataFrame
Pivot_df = df.groupBy('Company').pivot("Quarter").sum('Revenue')
df.show()
Pivot_df.show()



# Unpivot a DataFrame
# selecrExpr - In PySpark, selectExpr() is a DataFrame function used to select and transform columns using SQL expressions. It allows you to perform complex column-wise operations within your DataFrame similar to SQL's SELECT statement with SQL expressions.

Upf_df = Pivot_df.selectExpr("Company", "stack(4,'Q1','Q1','Q2','Q2','Q3','Q3','Q4','Q4') as (Quarter, Revenue)")

Upf_df.show()

















