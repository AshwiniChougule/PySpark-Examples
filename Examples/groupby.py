from pyspark import SparkContext
sc=SparkContext("local","Group By App")
x=sc.parallelize(['John','James','Arpit','Ankit'])
y=x.groupBy(lambda w:w[0])
for (k,v) in y.collect():
	print(k,list(v))

