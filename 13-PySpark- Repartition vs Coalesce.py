#adopting best partition strategy is designing best performance in spark applications
#The right number of partitions created based on number of cores boosts the performance , if not , hits the performance 
#evenly distributed partitions improves the performance, unevenly distributed performance hits the performance 
# lets say only one partition is created with size of 500 MB in a worker node with 16 cores. one partition cant be shared among cores. so one core would be processing 500 MB data where 15 cores are kept idle.



# Default partitions for RDD/Dataframe
'''
@The parameter sc.defaultParallelism determines the number of partitions when creating data within spark .
default value is 8 so it create 8 partition by default


@ When reading data from external system , partitions are created based on parameter spark.sql.files.maxPartitionBytes which is default 128 MB

Note - This is configurable 
'''
'''
Repartition - 
function repartition is used to increase or descrese partition in spark
Repartition always shuffle the data and build new partitionss from scratch
Repartition result i almost equal sizes partitions 
Due to full shuffle, it is not good for performance in some use cases. but as it creates equal sized partitions , good for performance in some use cases.

'''
'''
Coalese
coalesce function only reduce the number of partitions
coalesce doesnt require a full shuffle 
coalesce combines few partitions or shuffles data only from new partitions thus avoiding full shuffle 
due to partition merge, it produce uneven partitions
since full shuffle is avoided, coalesce is more performant than repartitions.
'''


# check default parameter
#The parameter sc.defaultParallelism determines the number of partitions when creating data within spark .
#default value is 8 so it create 8 partition by default


from pyspark.sql.types import StructType, StructField, StringType, IntegerType


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


#    sc.defaultParallelism


# check default parameter

 #When reading data from external system , partitions are created based on parameter spark.sql.files.maxPartitionBytes #which is default 128 MB

spark.conf.get("spark.sql.files.maxPartitionBytes")

#Out[3]: '134217728b'

# Generating data within spark enviornmnet 

# rdd = sc.parallelize(range(1,11))
# rdd.getNumPartitionss()

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10),IntegerType())

df.rdd.getNumPartitions()
#Out[4]: 8


# VERIFY THE DATA WITHIN ALL PARTITIONS

df.rdd.glom().collect()

'''Out[5]: [[Row(value=0)],
 [Row(value=1)],
 [Row(value=2)],
 [Row(value=3), Row(value=4)],
 [Row(value=5)],
 [Row(value=6)],
 [Row(value=7)],
 [Row(value=8), Row(value=9)]]
 '''


# Read Exterternal file

#File uploaded to /FileStore/tables/Mycsv/annual_enterprise_survey_2021_financial_year_provisional_size_bands_csv.csv
#File uploaded to /FileStore/tables/Mycsv/annual_enterprise_survey_2021_financial_year_provisional_csv__1_.csv


data = "/FileStore/tables/Mycsv/annual_enterprise_survey_2021_financial_year_provisional_size_bands_csv.csv"

df = spark.read.format("csv").option("header","true").load(data)
df.rdd.getNumPartitions()

#   Repartitions

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10),IntegerType())

df.rdd.glom().collect()
#df.rdd.getNumPartitions()

'''
Out[3]: [[Row(value=0)],
 [Row(value=1)],
 [Row(value=2)],
 [Row(value=3), Row(value=4)],
 [Row(value=5)],
 [Row(value=6)],
 [Row(value=7)],
 [Row(value=8), Row(value=9)]]
'''

df1 = df.repartition(20)       #we just get the 10 unique value but how we do create 20 partitions.
df1.rdd.getNumPartitions()

#when we dont have enough data to create partitiomns then it will create empty partitions
#here you ca see that only then partitions and other artitions are empty
df1.rdd.glom().collect()



df1 = df.repartition(2)

df1.rdd.getNumPartitions()

df1.rdd.glom().collect()


# Coalesce - used to reduce the number of partitions


df2 = df.coalesce(2)
df2.rdd.getNumPartitions()

df2.rdd.glom().collect()
























