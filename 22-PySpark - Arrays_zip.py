# What is Array_ZIP ?

# PySpark function that returns a merged array of structs in which the N-th struct contains all N-th value of input arrays


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

array_data = [
    ('John', 4, 41),
    ('John', 6, 2),
    ('David', 7, 3),
    ('Mike', 3, 4),
    ('David', 5, 2),
    ('John', 1, 3),
    ('John', 7, 3),
    ('David', 5, 3),
    ('David', 6, 1),
    ('David', 5, 2),
    ('Mike', 4, 5),
    ('Mike', 5, 2),
    ('Mike', 1, 5),
    ('John', 7, 3),
    ('David', 5, 2)
   
]

array_schema = ['Name','Score_1','Score_2']

arrayDF = spark.createDataFrame(data= array_data, schema=array_schema)
arrayDF.show()


# Convert sample dataframe into Array Dataframe

from pyspark.sql import functions as F

masterDF = arrayDF.groupBy('Name').agg(F.collect_list('Score_1').alias('Array_score_1'), F.collect_list('Score_2').alias('Array_score_2'))

masterDF.show()

masterDF.printSchema()

'''
 |-- Name: string (nullable = true)
 |-- Array_score_1: array (nullable = false)
 |    |-- element: long (containsNull = false)
 |-- Array_score_2: array (nullable = false)
 |    |-- element: long (containsNull = false)

'''


# Apply array_zip function on Array DF 

arr_zip_df = masterDF.withColumn('Zipped_value',F.arrays_zip('Array_score_1','Array_score_2'))

arr_zip_df.show(10,False)

'''
+-----+------------------+------------------+------------------------------------------------+
|Name |Array_score_1     |Array_score_2     |Zipped_value                                    |
+-----+------------------+------------------+------------------------------------------------+
|John |[4, 6, 1, 7, 7]   |[41, 2, 3, 3, 3]  |[{4, 41}, {6, 2}, {1, 3}, {7, 3}, {7, 3}]       |
|David|[7, 5, 5, 6, 5, 5]|[3, 2, 3, 1, 2, 2]|[{7, 3}, {5, 2}, {5, 3}, {6, 1}, {5, 2}, {5, 2}]|
|Mike |[3, 4, 5, 1]      |[4, 5, 2, 5]      |[{3, 4}, {4, 5}, {5, 2}, {1, 5}]                |
+-----+------------------+------------------+------------------------------------------------+

'''

# Practicle use case to flatten data using arrays_zip and example

DF1 = [
    ('Sales_dept',[{'emp_name':'John', 'Slary':'1000','yrs_of_service':'10','Age':'33'},
                    {'emp_name':'David', 'Slary':'4000','yrs_of_service':'20','Age':'43'},
                    {'emp_name':'Nancy', 'Slary':'5000','yrs_of_service':'30','Age':'53'},
                    {'emp_name':'Mike', 'Slary':'6000','yrs_of_service':'40','Age':'63'},
                    {'emp_name':'Rosy', 'Slary':'7000','yrs_of_service':'50','Age':'73'}])
    ('HR_Dept',[{'emp_name':'Kelvin', 'Slary':'1000','yrs_of_service':'10','Age':'33'},
                 {'emp_name':'David', 'Slary':'1000','yrs_of_service':'10','Age':'33'},
                 {'emp_name':'Warner', 'Slary':'1000','yrs_of_service':'10','Age':'33'},
                 {'emp_name':'Starck', 'Slary':'1000','yrs_of_service':'10','Age':'33'},
                 {'emp_name':'Hohnsen', 'Slary':'1000','yrs_of_service':'10','Age':'33'}])]
                 

se_chema = ['Department','Employee']

df_brand = spark.createDataFrame(data=DF1, schema=se_chema)
df_brand.show()


