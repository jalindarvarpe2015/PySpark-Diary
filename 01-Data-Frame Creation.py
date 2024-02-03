# Ways To create Dataframe
#USING LIST OF VALUES

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()
data = [
    ('Ajay','sangamner',25,'Mechanical'),
    ('Vijay','Nashik',45,'Computer'),
    ('Sagar','Shirdi',34,'Mechanical'),
    ('Sanket','Akole',26,'Diploma'),
    ('Ramesh','Pune',25,'Mechanical')
]


df = spark.createDataFrame(data = data, schema = ['Name','city','Age','Degree'])
df.show()