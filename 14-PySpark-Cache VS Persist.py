# Cache - Cache is programming mechanism which gives an option to store the data in memory across nodes
# default storage lvel is memory only



#Persist - Persisit is programming mechanism which gives an option to store the data either in memory or in disc across nodes
'''storage level 
memory_only ----> same as cache
momory_only_ser. ----> persisting the rdd in serilized (binary) from helps to reduce the size of the rdd, thus making space for more rdd to be persisted in cache memory .it is space efficient but not time-efficient
memory_and_disk ----> stores partitions on disk which do not fit in memory 
memory_and_disk_ser ----> same as memory and disk but in serialized format
disk_only ---> persist data only in disk, which would require network input and output operations thus time consuming . still better performance than re-computation each time through DAG

DISK_only_2 ----> same as above methods but creates replication in another node. so if any failure of node , still  
                    data is accessible through another node    
MEMORY_AND_DISK_2 
MEMORY_ONLY_2 
MEMORY_AND_DISK_DER_2
MEMORY_ONLY_SER_2


# when we trigger the action then n then only in memory computation start. but if you want to store that data in memory or disc then we can use this cache and persist.
'''

# cache syntax
'''
rdd.cache()
df.cache()
'''


# Persist Syntax
'''
rdd.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.MEMORY_ONLY_SER)

'''
