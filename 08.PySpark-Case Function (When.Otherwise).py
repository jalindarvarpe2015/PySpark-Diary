'''
 When Oyherwise 

To evaluate list of condition and choose a result path according to the ,atching conditions , when ()otherwise ()
function in pyspark can be used 

This is similar to casse or switch statement in ther programming languae 


When no condition is matching ,Otherwise result path would be chosen
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()



data_student = [("Raja","Science", 80, "P", 90),
                ("Rakesh","Maths", 90, "P", 70),
                ("Rama","English", 20, "F", 80),
                ("Ramesh","Science", 45, "F", 75),
                ("Rajesh","Maths", 30, "F", 50),
                ("Raghav","Maths", None, "NA", 70)]


Schema = ["Name","Subject","Mark", "Status", "Attendance"]

df = spark.createDataFrame(data=data_student, schema=Schema)
df.show()


''' Update The existing Column '''


from pyspark.sql.functions import when

df1 = df.withColumn("status",when (df.Mark >= 50, "Pass" )
                            .when (df.Mark <50, "Fail")
                            .otherwise("Absent"))
df1.show()



'''Create A New Column '''

df2 = df.withColumn("New_Status",when(df.Mark>50, "Pass")
                                .when(df.Mark<50, "Fail")
                                .otherwise("Absent"))
df2.show()


''' Multi Conditions using AND and OR Operator'''



df4 = df.withColumn("Grade", when((df.Mark>= 80) & (df.Attendance >= 80), "Distnction")
                                 .when ((df.Mark>=50 ) & (df.Attendance >= 50), "Good")
                                 .otherwise("Average"))
                           

df4.show()


''' Multi Conditions using AND and OR Operator'''
''' OR  or | '''


df4 = df.withColumn("Grade", when((df.Mark>= 80) | (df.Attendance >= 80), "Distnction")
                                 .when ((df.Mark>=50 ) | (df.Attendance >= 50), "Good")
                                 .otherwise("Average"))
                           

df4.show()



