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


Write Modes in Spark:

1 : Append :
add the new data to the existing data . if the destination already has data , spark will apped the new data to it.iter

 Example: 
If you’re writing to a Parquet file and the file already has data, Spark will add the new data without overwriting the
 existing data.


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("AppendExample").getOrCreate()

# Sample DataFrame
data = [("John", 30), ("Alice", 25)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Write DataFrame to a CSV file in append mode
df.write.mode("append").csv("path/to/output.csv")

# If you are writing to a Parquet file
df.write.mode("append").parquet("path/to/output.parquet")

2.overwrite:
Description: 
Replaces the existing data with the new data. 
If the destination already has data, it will be replaced by the new data.

 Example: 
If you’re writing to a CSV file and it already exists, Spark will overwrite it with the new data.

# Write DataFrame to a CSV file in overwrite mode
df.write.mode("overwrite").csv("path/to/output.csv")

# If you are writing to a Parquet file
df.write.mode("overwrite").parquet("path/to/output.parquet")


3.ignore:
 Description: 
If the destination already has data, Spark will ignore the write operation and leave the existing data
 unchanged.
 Example: 
If you attempt to write to an existing Parquet file with this mode, the file will remain unchanged if it
 already exists.

 # Write DataFrame to a CSV file in ignore mode
df.write.mode("ignore").csv("path/to/output.csv")

# If you are writing to a Parquet file
df.write.mode("ignore").parquet("path/to/output.parquet")

4. error or errorifexists:
 Description: 
The default mode. 
If the destination already has data, Spark will throw an error and not perform the write
 operation.
 Example:
 If you try to write to a CSV file that already exists, Spark will throw an error rather than
 overwriting or ignoring the file.

# Write DataFrame to a CSV file in error mode
df.write.mode("error").csv("path/to/output.csv")

# Alternatively, you can use errorifexists (they are equivalent)
df.write.mode("errorifexists").csv("path/to/output.csv")