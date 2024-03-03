# UDF 
# User Defined Function (UDF) is piece of code which perform certain task and can be re-used to perform the same task across multiple scenarios

# UDF - Black Box
# (Optimization caan not apply on UDF)

# - UDF IS expensive operation in spark development
# - Try to minimize the usage of UDF and apply built in functions whenever possible 
# - UDF are created in python or scala but dataframe are in JVM format. so when we call UDF to execute certain task, it would happen through java API, which require data serilization/deserilization to perform task . and as UDF is black box to spark (as not in JVM), IT cannt apply optimization by default # UDF 
# User Defined Function (UDF) is piece of code which perform certain task and can be re-used to perform the same task across multiple scenarios

# UDF - Black Box
# (Optimization caan not apply on UDF)

# - UDF IS expensive operation in spark development
# - Try to minimize the usage of UDF and apply built in functions whenever possible 
# - UDF are created in python or scala but dataframe are in JVM format. so when we call UDF to execute certain task, it would happen through java API, which require data serilization/deserilization to perform task . and as UDF is black box to spark (as not in JVM), IT cannt apply optimization by default 


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()


employee_data =[
    (101, "John Doe", "2003-03-01", 1, 60000),
    (102, "Jane Smith", "2003-03-02", 2, None),
    (103, "Bob Johnson","2003-03-03" , 1, 70000),
    (104, "Alice Brown","2003-03-04" , 3, None),
    (105, "Charlie Wilson", "2003-03-05", 2, 90000)]

employee_schema = ['employee_id','name','doj','employee_dept','salary']

empDF = spark.createDataFrame(data=employee_data, schema=employee_schema)

empDF.show()


# Define UDF TO Rename Columns
import pyspark.sql.functions as f

def rename_columns(rename_df):

    for column in rename_df.columns:

        new_column = "col_" + column
        rename_df = rename_df.withColumnRenamed(column, new_column)

    return rename_df

# Execute UDF

rename_df = rename_columns(empDF)
rename_df.show()


# UDF to convert name into upper case 

from pyspark.sql.functions import upper , col

def upperCase_col(df):

    empDF_upper = df.withColumn('name_upper', upper(df.name))

    return empDF_upper


up_case_df = upperCase_col(empDF)
up_case_df.show()





