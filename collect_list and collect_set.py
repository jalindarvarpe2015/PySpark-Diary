# In PySpark, collect_list and collect_set are useful functions for aggregating data. 

collect_list
Purpose: Collects and returns a list of values.
Usage: Useful when you want to retain duplicates and maintain the order of elements.
Example:
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

spark = SparkSession.builder.appName("example").getOrCreate()
data = [("Alice", 1), ("Bob", 2), ("Alice", 3)]
df = spark.createDataFrame(data, ["name", "value"])

df.groupBy("name").agg(collect_list("value").alias("values")).show()
This will output:
+-----+---------+
| name|   values|
+-----+---------+
|  Bob|      [2]|
|Alice|[1, 3]|
+-----+---------+
collect_set
Purpose: Collects and returns a set of unique values.
Usage: Useful when you want to remove duplicates.
Example:
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set

spark = SparkSession.builder.appName("example").getOrCreate()
data = [("Alice", 1), ("Bob", 2), ("Alice", 3), ("Alice", 1)]
df = spark.createDataFrame(data, ["name", "value"])

df.groupBy("name").agg(collect_set("value").alias("values")).show()
This will output:
+-----+---------+
| name|   values|
+-----+---------+
|  Bob|      [2]|
|Alice|[1, 3]|
+-----+---------+
