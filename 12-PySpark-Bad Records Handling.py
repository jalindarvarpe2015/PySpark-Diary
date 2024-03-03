'''

 Corrupt records Handling
#1) Premissive - include corrupt record in seperate column
PERMISSIVE Mode: (Default mode)

In this mode, PySpark loads corrupt records as null values and adds a "_corrupt_record" column to the DataFrame to hold the raw content of the corrupted record.



#2) Drop Mulformed - ignore corrupt records

DROPMALFORMED Mode:

This mode drops the whole corrupted records during the reading process.


#3) Fail Fast - Throw Exception if corrupt record
FAILFAST Mode:

In this mode, PySpark throws an exception as soon as it encounters a corrupted record, preventing further processing.
'''

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

schema = StructType([
    StructField("month", StringType(), True),
    StructField("emp_no", IntegerType(), True),
    StructField("emp_count", IntegerType(), True),
    StructField("expense", IntegerType(), True),
    StructField("corrupt_record", StringType(), True)
])



data = "/FileStore/tables/csv/emp_data.csv"

df = spark.read.format("csv").option("header","true").option("inferschema","true").load(data)

df.show()









