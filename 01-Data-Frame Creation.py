# PySpark | How to Create a Dataframe?
# In PySpark, a DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database or an Excel spreadsheet.
# DataFrames provide a powerful abstraction for working with structured data, offering ease of use, high-level transformations, and optimization features like catalyst and Tungsten. This article will cover how to create a DataFrame in PySpark using different methods.

# There are several ways to create a DataFrame in PySpark:

# 1) From a list or a dictionary (i.e., from a collection).
# 2) From an external data source (e.g., CSV, JSON, Parquet, etc.).
# 3) From an RDD (Resilient Distributed Dataset).

# 1. Creating a DataFrame from a List or Dictionary: One of the simplest ways to create a DataFrame is from a Python list or dictionary.
#PySpark’s createDataFrame() method can be used to convert these collections into DataFrames.

# Import necessary modules
from pyspark.sql import SparkSession
 
# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()
 
# Creating a DataFrame from a list of tuples
data = [("John", 30), ("Jane", 28), ("Sam", 35)]
 
columns = ["Name", "Age"]
 
# Creating the DataFrame with schema
df_with_schema = spark.createDataFrame(data, columns)
 
# Display the DataFrame content
df_with_schema.show()

# Explanation:

# createDataFrame(): This method is used to convert a collection (such as a list of tuples) into a DataFrame.
# df.show(): Displays the contents of the DataFrame.

# Creating a DataFrame from a list of dictionaries
data = [{"Name": "John", "Age": 30}, {"Name": "Jane", "Age": 28}, {"Name": "Sam", "Age": 35}]
 
# Creating the DataFrame
df = spark.createDataFrame(data)
 
# Display the DataFrame
df.show()

# 2. Creating a DataFrame from External Data Sources: You can also create a DataFrame by reading data from external sources like CSV, JSON, Parquet, or databases. PySpark provides methods to load data directly into DataFrames.

# a) Creating a DataFrame from a CSV File:

# Reading a CSV file into a DataFrame
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
 
# Display the DataFrame
df.show()


# Explanation:

# spark.read.csv(): This method reads a CSV file and creates a DataFrame.
# header=True: Indicates that the first row contains the column names.
# inferSchema=True: Automatically infers the data types of the columns


# b) Creating a DataFrame from a JSON File:

# Reading a JSON file into a DataFrame
df = spark.read.json("path/to/file.json")
 
# Display the DataFrame
df.show()
# c) Creating a DataFrame from a Parquet File:

# Reading a Parquet file into a DataFrame
df = spark.read.parquet("path/to/file.parquet")
 
# Display the DataFrame
df.show()


# 3. Creating a DataFrame from an RDD: If you already have an RDD, you can easily convert it into a DataFrame by using the toDF() method or the createDataFrame() method.

# Creating an RDD
rdd = spark.sparkContext.parallelize([("John", 30), ("Jane", 28), ("Sam", 35)])
 
# Converting the RDD into a DataFrame
df = rdd.toDF(["Name", "Age"])
 
# Display the DataFrame
df.show()

# Explanation:

# toDF(): This method is used to convert an RDD into a DataFrame. You can specify the column names as an argument.
# Conclusion: Creating a DataFrame in PySpark is a crucial first step when working with structured data. DataFrames offer an easy-to-use API and are optimized for distributed computing, making them ideal for large-scale data processing. In this guide, we covered three main ways to create a DataFrame: from a collection (such as a list), from an external data source (like CSV, JSON, or Parquet), and from an existing RDD.