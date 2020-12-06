from pyspark import SparkContext
sc=SparkContext("local","MAP Partition with key APP")
x = sc.parallelize([1,2,3], 2)
def f(partitionIndex, iterator): yield (partitionIndex, sum(iterator)) 
y = x.mapPartitionsWithIndex(f)
# glom() flattens elements on the same partition 
print(x.glom().collect()) 
print(y.glom().collect())
