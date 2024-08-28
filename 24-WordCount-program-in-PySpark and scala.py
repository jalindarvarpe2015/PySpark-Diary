# Word count program in scala 

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val textFile = sc.textFile("demoscala.txt")
val counts = textFile
  .flatMap(line => line.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)

// Collect the results and print each word with its count
counts.collect().foreach { case (word, count) =>
  println(s"$word: $count")
}


# word count program in scala and python 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Create an RDD with the list of words
words = ["java", "scala", "python", "c++", "python", "scala", "scal"]
rdd = spark.sparkContext.parallelize(words)

# Convert RDD to DataFrame with a single column named "word"
df = rdd.map(lambda word: (word,)).toDF(["word"])

# Count occurrences of each word
word_counts = df.groupBy("word").agg(count("word").alias("count"))

# Sort by count in descending order
sorted_word_counts = word_counts.orderBy(col("count").desc())

# Show the result
sorted_word_counts.show(truncate=False)