from pyspark import SparkContext
sc=SparkContext("local","Map Partitions App")
x = sc.parallelize([1,2,3], 2)
def f(iterator): yield sum(iterator); yield 42 
y = x.mapPartitions(f)
# glom() flattens elements on the same partition 
print(x.glom().collect()) 
print(y.glom().collect())
