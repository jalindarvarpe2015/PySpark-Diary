# Handling Null 

# na.drop() --> Drops the rows if any or all columns contail Null Value

# df1 = df.na.drop("parameter")
# parameter: "any","all", "subset=["column1","column2,...."]"

# Alternate Syntax : 
# df1 = df.dropna("parameter")


# na.fill --> Populate dummy value for all Null values given as parameter tto this function

# df1 = df.na.fill(value="dummy value", subset=["column"])

# df1 = df.fillna("parameter")



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


data = [
    ("Alice", "Math", 85, "Pass", 92),
    ("Bob", "English", 90, "Pass",None),
    ("Charlie", "Science", 92, "Fail", 80),
    ("David", "History", None, "Pass", None),
    ("Eva", "Computer Science", None, None, 80),
    (None, None, None, None, None)]

schema = ['name','subject','mark','status','attendence']
df = spark.createDataFrame(data= data, schema=schema)
df.show()


# Drop the records with Null Value - ALL & ANY
df1 = df.na.drop()     # default it take any
df1.show()

#df1 = df.na.drop("any")
#display(df1)

#df1 = df.dropna() # Alternate Syntax

# "all" ----> all the values in record is null then it will remove only that records

df1 = df.na.drop("all")
df1.show()

# Drop the records with Null Value on selected column

df1 = df.na.drop(subset=['mark']) #it could be multiole column
df1.show()


# Drop the records with Null Value on multiole  columns

df1 = df.na.drop(subset=['mark','attendence'])
df1.show()

#   Fill value for Specific columns if contains Null

df1 = df.na.fill(value=0) # zero is integer value it will not applicable for string columns
df1.show()

#   Fill value for Specific columns if contains Null

df1 = df.na.fill(value='NA') # SINCE NA is STRING value it will not applicable for INTEGER columns
df1.show()

# Fill Value for specific Columns if Contains Null

df1 = df.na.fill(value=0, subset=['mark','attendence'])
df1.show()

# Fill Value for specific Columns if Contains Null

df1 = df.fillna({"mark": 0, "status":"na","name":"no_name","subject":"english","attendence":50})
df1.show()











