from pyspark.sql import SparkSession, DataFrame

def proccess_batch(df: DataFrame, batch_id: int):
    (df
     .filter("topic = 'ark-topic-1'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-1'))
    (df
     .filter("topic = 'ark-topic-2'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-2'))

spark = (SparkSession
         .builder
         .appName('quickstart-streaming-kafka')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

source = (spark
          .readStream
          .format('kafka')
          .option('kafka.bootstrap.servers', 'localhost:9092')
          .option('subscribe', 'ark-topic-1,ark-topic-2')
          .option("startingOffsets", "earliest")
          .option('checkpointLocation', '/home/aramis2008/sparkstreamingFromKafka/checkpoint')
          .load())

(source
     .writeStream
     .foreachBatch(proccess_batch)
     .start()
     .awaitTermination())
