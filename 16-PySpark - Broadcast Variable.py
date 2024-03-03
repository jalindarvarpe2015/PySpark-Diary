# Broadcast Variable
#Performance Tuning

#what is broadcast Variable?
'''
it is programming mechanism in spark, through which we can keep read-only copy of data into each node of cluster instead of sending it to node every time a task needs it.

'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("example").getOrCreate()

# create sample dataframe 

Tranction = [{"store_id": 101, "item": "Phone", "amount": 799.99},
{"store_id": 102, "item": "Laptop", "amount": 1499.99},
{"store_id": 103, "item": "Headphones", "amount": 99.99},
{"store_id": 104, "item": "Smartwatch", "amount": 199.99},
{"store_id": 105, "item": "Camera", "amount": 899.99},
{"store_id": 101, "item": "Tablet", "amount": 499.99},
{"store_id": 102, "item": "Television", "amount": 1299.99},
{"store_id": 103, "item": "Gaming Console", "amount": 299.99},
{"store_id": 104, "item": "Desktop Computer", "amount": 999.99},
{"store_id": 115, "item": "Wireless Earbuds", "amount": 79.99}]

TranctionDF = spark.createDataFrame(Tranction)

Tranction.show()

# Create sample dimension table 

store = [
    (101,'store_london'),
    (102,'store_paris'),
    (103,'store_uk'),
    (104,'stork_germeny'),
    (105,'store_aus')
    ]

storeDF = spark.createDataFrame(data = store, schema =['Store_id','Store_name'])
storeDF.show()

from pyspark.sql.functions import broadcast 

joinedDF = TranctionDF.join(broadcast(storeDF), TranctionDF['Store_id']==storeDF['store_id'])
joinedDF.show()

joinedDF.explain()

'''
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(2) BroadcastHashJoin [Store_id#23L], [store_id#53L], Inner, BuildRight, false
   :- *(2) Filter isnotnull(Store_id#23L)
   :  +- *(2) Scan ExistingRDD[amount#21,item#22,store_id#23L]
   +- ShuffleQueryStage 0, Statistics(sizeInBytes=192.0 B, rowCount=5, isRuntime=true)
      +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=215]
         +- *(1) Filter isnotnull(store_id#53L)
            +- *(1) Scan ExistingRDD[Store_id#53L,Store_name#54]
+- == Initial Plan ==
   BroadcastHashJoin [Store_id#23L], [store_id#53L], Inner, BuildRight, false
   :- Filter isnotnull(Store_id#23L)
   :  +- Scan ExistingRDD[amount#21,item#22,store_id#23L]
   +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=180]
      +- Filter isnotnull(store_id#53L)
         +- Scan ExistingRDD[Store_id#53L,Store_name#54


'''


joinedDF.explain(True)

'''  +- LogicalRDD [amount#21, item#22, store_id#23L], false
+- Filter isnotnull(store_id#53L)
   +- LogicalRDD [Store_id#53L, Store_name#54], false

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(2) BroadcastHashJoin [Store_id#23L], [store_id#53L], Inner, BuildRight, false
   :- *(2) Filter isnotnull(Store_id#23L)
   :  +- *(2) Scan ExistingRDD[amount#21,item#22,store_id#23L]
   +- ShuffleQueryStage 0, Statistics(sizeInBytes=192.0 B, rowCount=5, isRuntime=true)
      +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=215]
         +- *(1) Filter isnotnull(store_id#53L)
            +- *(1) Scan ExistingRDD[Store_id#53L,Store_name#54]
+- == Initial Plan ==
   BroadcastHashJoin [Store_id#23L], [store_id#53L], Inner, BuildRight, false
   :- Filter isnotnull(Store_id#23L)
   :  +- Scan ExistingRDD[amount#21,item#22,store_id#23L]
   +- Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=180]
      +- Filter isnotnull(store_id#53L)
         +- Scan ExistingRDD[Store_id#53L,Store_name#54]

'''











