# Reads Modes iNPySpark

1 : PREMISSIVE (Default)
Description : allows spark to handle corrupted records by setting them as null. If a record cannot be parced,spark wont throw an error but will set that field to null


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

df = spark.read.option("mode", "PERMISSIVE").csv("path/to/file.csv")
df.show()


2 : DROPMALFORMED :
Descripion :
Drops rows with corrupted or mulformed Data.
These rows will not appear in the resulting dataframe.

Example : if you have a CSV file with a row that doesnt match the excepted schema, that row will be exculded from the dataframe 

df = spark.read.option("mode", "DROPMALFORMED").csv("path/to/file.csv")


3 : FAILFAST : 
Description :
Throws an error as soon as it encounters corrupted or mulformed Data.
This Mode is used when you want to ensure that no bad data is processed. 

Example :
if a CSV file has a row with unexcepted format, spark will fail immediately , and you will need to fix the data or handle it seperately 

df = spark.read.option("mode", "FAILFAST").csv("path/to/file.csv")
