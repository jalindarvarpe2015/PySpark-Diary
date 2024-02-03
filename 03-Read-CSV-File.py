# Read CSV File 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


data = "/FileStore/tables/mycsvdata/annual_enterprise_survey_2021_financial_year_provisional_csv.csv"

df = spark.read.format("csv").option("header","true").option("inferschema","true").load(data)

df.show()
