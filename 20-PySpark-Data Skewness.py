# What Is Data Skew

# Data Skew is a condition in which a tables data is unevenly distributed among partitions in the cluster
# Data Skew can severely downgrade performance of queries, especially those with joins 
# Joins between big tables require shuffling data and the skew can lead to an extreme imbalance of work in the cluster
# It is likely that data skew is affecting a query if a query apprears to be stuck finishing very few task (For example , the last 3 tasks out of 200)


# How to handle Data Skew
 
 # 1- Salt the skewed column : with a random number creating a better distribution across each partition
 # 2- Apply Skew-Hint : with the information from these hints, spark can construct a better query plan, one that does not suffer from data skew
 # 3 - Use Broadcast join : for smaller table
 # 4 - Enable Adaptive query execution : if you are using spark 3 which will balance out the partitions for us automatically


# Configure skew hint with relation name and column names
# A skew hint must contain at least the name of the relation with skew. A relation is a table, view, or a subquery. All joins with this relation then use skew join optimization.

'''

-- table with skew
SELECT /*+ SKEW('orders') */
  *
  FROM orders, customers
  WHERE c_custId = o_custId

-- subquery with skew
SELECT /*+ SKEW('C1') */
  *
  FROM (SELECT * FROM customers WHERE c_custId < 100) C1, orders
  WHERE C1.c_custId = o_custId

'''

#Configure skew hint with relation name and column names

# There might be multiple joins on a relation and only some of them will suffer from skew. Skew join optimization has some overhead so it is better to use it only when needed. For this purpose, the skew hint accepts column names. Only joins with these columns use skew join optimization.

'''
-- single column
SELECT /*+ SKEW('orders', 'o_custId') */
  *
  FROM orders, customers
  WHERE o_custId = c_custId

-- multiple columns
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId')) */
  *
  FROM orders, customers
  WHERE o_custId = c_custId AND o_storeRegionId = c_regionId

'''

#Configure skew hint with relation name, column names, and skew values

# You can also specify skew values in the hint. Depending on the query and data, the skew values might be known (for example, because they never change) or might be easy to find out. Doing this reduces the overhead of skew join optimization. Otherwise, Delta Lake detects them automatically.

'''
-- single column, single skew value
SELECT /*+ SKEW('orders', 'o_custId', 0) */
  *
  FROM orders, customers
  WHERE o_custId = c_custId

-- single column, multiple skew values
SELECT /*+ SKEW('orders', 'o_custId', (0, 1, 2)) */
  *
  FROM orders, customers
  WHERE o_custId = c_custId

-- multiple columns, multiple skew values
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId'), ((0, 1001), (1, 1002))) */
  *
  FROM orders, customers
  WHERE o_custId = c_custId AND o_storeRegionId = c_regionId

'''


# AQE (Adaptive Query Execution)

# Dynamically coalesce shuffle partition
# Dynamically switch join Strategies
# Dynamically optimized joins