data ="/FileStore/tables/asl-7.csv"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


df=spark.read.format("csv").option("header","true").load(data)
df.show()



