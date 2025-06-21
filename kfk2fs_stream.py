from time import sleep
from threading import Thread
from pyspark.sql import SparkSession, DataFrame

def proccess_batch_1(df: DataFrame, batch_id: int):
    print(df.count(), batch_id)
    (df
     .filter("topic = 'ark-topic-1'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-1'))

def proccess_batch_2(df: DataFrame, batch_id: int):
    print(df.count(), batch_id)
    (df
     .filter("topic = 'ark-topic-2'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-2'))

def kafka_write(process_type):
    print(f'TOLCH --{process_type = }-- TOLCH')
    if process_type == "1":
        process_func = proccess_batch_1
    elif process_type == "2":
        process_func = proccess_batch_2
    else:
        raise Exception("bad type")

    #.appName('quickstart-streaming-kafka')
    spark = (SparkSession
             .builder
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

    df_topic = (df.writeStream
         .foreachBatch(process_func)
         .start())

    df_topic.awaitTermination()

print('start 1 func')
thread1 = Thread(target=kafka_write, args='1')
thread1.start()
print('sleep 70 sec')
sleep(70)
print('start 2 func')
thread2 = Thread(target=kafka_write, args='2')
thread2.start()

print('NASTCH ---- NASTCH')