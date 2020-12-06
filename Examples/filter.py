from pyspark import SparkContext
sc=SparkContext("local", "Filter App")
x=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
y=x.filter(lambda z: z%2==1)
print(x.collect())
print(y.collect())

