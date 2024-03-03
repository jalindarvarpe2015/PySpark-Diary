# df = spark.read.option("multiline","true").json('FILE_PATH')


data = "/FileStore/tables/json/world_bank.json"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


df = spark.read.format('json').option("seperator",";").option("multiline","true").load(data)
df.show()


