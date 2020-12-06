from pyspark import SparkContext
logFile="file:////Users/gautam/Downloads/Spark/spark-3.0.0-preview2-bin-hadoop3.2/README.md"
sc = SparkContext("local", "app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
