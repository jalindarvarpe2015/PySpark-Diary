'''
 Explode Function in PySpark

 It is a PySpark function that return  new row for each element in the given array or map
array - List of Element

map - list of key and value pair

uses the default column name col for element in the array and key and value for element in the map '''

''' Explode have 4 varients

1) explode
when an array is passed  to this function it creates new row for each element in array.
when map is passed it creates two new column one for key and second for value and each element in map split into the rows.
if the array or map is null that row is eliminated.

2)explode_outer
unlike explode, if the array or map is null explode outer return null'

3)posexlode
when array or map is passed it creates positional columnn also for each element ignore the null element 

4)posexplode.
unlike posexplode, if the array or map is null explode_outer return null.
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

array_appliance = [
        ('Raja', ['TV', 'Refrigerator', 'Oven', 'AC']),
        ('Raghav', ['AC', 'Washing Machine', None]),
        ('Ram', ['Grinder', 'TV']),
        ('Ramesh', ['Refrigerator', 'TV', None]),
        ('Rajesh', None)]

df_app = spark.createDataFrame(data=array_appliance, schema=['Name','Applience'])
df_app.printSchema()
display(df_app)


map_brand = [
        ('Raja', {'TV':'LG', 'Refrigerator':'Samsung', 'Oven':'Philipps', 'AC':'Voltas'}),
        ('Raghav', {'AC':'Samsung', 'Washing Machine':'LG'}),
        ('Ram', {'Grinder':'Preethi', 'TV':''}),
        ('Ramesh', {'Refrigerator':'LG', 'TV':'Croma'}),
        ('Rajesh', None)]

df_brand = spark.createDataFrame(data=map_brand,schema=['Name','Brand'])
df_brand.printSchema()
display(df_brand)


'''1) explode -- when array is passed
when an array is passed  to this function it creates new row for each element in array.
when map is passed it creates two new column one for key and second for value and each element in map split into the rows.
if the array or map is null that row is eliminated.'''


from pyspark.sql.functions import explode


df1 = df_app.select(df_app.Name, explode(df_app.Applience))
df1.printSchema()
display(df1)


df_app.printSchema()
display(df_app)



'''1) explode -- when map is passed
when an array is passed  to this function it creates new row for each element in array.
when map is passed it creates two new column one for key and second for value and each element in map split into the rows.
if the array or map is null that row is eliminated.'''


from pyspark.sql.functions import explode

df2= df_brand.select(df_brand.Name, explode(df_brand.Brand))
display(df2)
df2.printSchema()

df_brand.printSchema()
display(df_brand)



'''2)explode_outer
unlike explode, if the array or map is null explode outer return null'''

from pyspark.sql.functions import explode_outer


display(df_app.select(df_app.Name,explode_outer(df_app.Applience)))

display(df_brand.select(df_brand.Name, explode_outer(df_brand.Brand)))


'''
3)posexlode
when array or map is passed it creates positional columnn also for each element ignore the null element 
'''

from pyspark.sql.functions import posexplode

display(df_app.select(df_app.Name,posexplode(df_app.Applience)))

display(df_brand.select(df_brand.Name, posexplode(df_brand.Brand)))


'''
4)posexplode
unlike posexplode, if the array or map is null explode_outer return null
'''

from pyspark.sql.functions import posexplode_outer

display(df_app.select(df_app.Name,posexplode_outer(df_app.Applience)))

display(df_brand.select(df_brand.name, posexplode_outer(df_brand.Brand)))



