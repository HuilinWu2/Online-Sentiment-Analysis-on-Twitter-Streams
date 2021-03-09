from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


# create the Streaming Context from the above spark context with interval size 3 seconds
ssc = StreamingContext(sc, 3)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9800
dataStream = ssc.socketTextStream("localhost",9800)


# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))


#
hashtags = words.filter(lambda w: 'slow' or 'fast' in w).count()
hashtags.pprint()



# start and wait for stopping
ssc.start()
ssc.awaitTermination()
