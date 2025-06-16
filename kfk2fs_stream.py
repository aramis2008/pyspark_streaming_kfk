from pyspark.sql import SparkSession, DataFrame

def proccess_batch(df: DataFrame, batch_id: int):
    print(df.count(), batch_id)
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
    (df
     .filter("topic = 'ark-topic-3'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-3'))

spark = (SparkSession
         .builder
         .appName('quickstart-streaming-kafka')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')

df = (spark
          .readStream
          .format('kafka')
          .option('kafka.bootstrap.servers', 'localhost:9092')
          .option('subscribe', 'ark-topic-1,ark-topic-2,ark-topic-3')
          .option("startingOffsets", "earliest")
          .option('checkpointLocation', '/home/aramis2008/sparkstreamingFromKafka/checkpoint')
          .load())
print('TOLCH ---- TOLCH')

df2 = (df.writeStream
     .foreachBatch(proccess_batch)
     .start())

# Все вычитает и закончится если даже поступают новые.
# Без нижних команды не происходит процесс забора, просто остановка.
# stop() - как-будто ни на что не влияет
# загружает все заново при каждом запуске.
df2.processAllAvailable()
df2.stop()

print('NASTCH ---- NASTCH')