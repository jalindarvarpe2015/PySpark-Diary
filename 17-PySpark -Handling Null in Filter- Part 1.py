#Handling Nulll in filter


# isNull() --> the function isNull() returns all the rows where certain column on which we apply this function contains null value

#isNotNull() ---> the function isNotNull() create new datafrane by eliminating all the rows where certain column on which we apply this function contails null value

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

data = [
    ("Alice", "Math", 85, "Pass", 92),
    ("Bob", "English", None, "Incomplete", 75),
    ("Charlie", "Science", 92, "Pass", None),
    ("David", "History", 78, "Pass", 95),
    ("Eva", "Computer Science", None, "Incomplete", 80)]

df = spark.createDataFrame(data, schema=['student','subject','mark','status','attendence'])

df.show()

#isNull() 

#df1 = df.filter(df.mark.isNull())
#display(df1)


#df1 = df.filter("mark is Null")     
#display(df1)


#from pyspark.sql.functions import col
#display(df.filter(col('mark').isNull()))


#isNotNull()

df2 = df.filter(df.mark.isNotNull())
df2.show()

#df2 = df.filter("mark isNotNull")     
#display(df1)


#from pyspark.sql.functions import col
#display(df.filter(col('mark').isNotNull()))


# multiple conditions

df1 = df.filter(df.mark.isNotNull())
df1.show()

df1 = df.filter(df.mark.isNotNull() & (df.attendence.isNotNull()))
df1.show()

df1 = df.filter(df.mark.isNotNull() | (df.attendence.isNotNull()))
df1.show()




