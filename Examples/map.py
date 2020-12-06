from pyspark import SparkContext
sc = SparkContext("local", "Map app")
words = sc.parallelize (
   ["hadoop", 
   "spark", 
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)
words2=words.map(lambda x:(x,1))
print(words.collect())
print(words2.collect())
