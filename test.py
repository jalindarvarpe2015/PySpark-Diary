import pyspark

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Test") \
    .getOrCreate()

# Sample data (if you don't have a CSV file, you can create a DataFrame from this list)
data = [("James", "Smith", "USA", 30),
        ("Michael", "Rose", "USA", 28),
        ("Robert", "Williams", "UK", 25),
        ("Maria", "Jones", "UK", 35)]

# Define schema
columns = ["First_Name", "Last_Name", "Country", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
print("Initial DataFrame:")
df.show()