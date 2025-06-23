from time import sleep
from threading import Thread
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import  col, from_json, lit, schema_of_json


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
    # work well, другой поток не останавливается.
    # raise Exception("test session")

def proccess_batch_value_parse(df: DataFrame, batch_id: int):
    df_decode = df \
        .filter("topic = 'ark-topic-1'") \
        .select(col('value').cast('string'))
        #.select(decode(col('value'), "UTF-8").alias('value'))

    df_decode.show(10, truncate = False)

    schema_t = schema_of_json(lit(df_decode.select('value').first().value))
    df_full = df_decode.withColumn('value', from_json(col('value'), schema_t)).select(col('value.*'))
    df_full.show(10, truncate = False)
    df_full.printSchema()


def kafka_write(process_type):
    print(f'TOLCH --{process_type = }-- TOLCH')
    if process_type == "1":
        process_func = proccess_batch_1
    elif process_type == "2":
        process_func = proccess_batch_2
    else:
        raise Exception("bad type")

    spark = (SparkSession
             .builder
             .appName('quickstart-streaming-kafka-0')
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
                .queryName(process_type)
                .foreachBatch(process_func)
                .start())

    df_topic.awaitTermination()
    # следующие команды не запускаются
    # print('here work?')
    # if process_type == "2":
    #     print('sleep 20 sec')
    #     sleep(20)
    #     df_topic.stop()


print('start 1 func')
thread1 = Thread(target=kafka_write, args='1')
thread1.start()
print('sleep 25 sec')
sleep(25)
print('start 2 func')
thread2 = Thread(target=kafka_write, args='2')
thread2.start()

# work well
print('sleep 15 sec')
sleep(15)
spark = SparkSession.builder.getOrCreate()
print(spark.streams.active[0].name)
print(spark.streams.active[1].name)
print('stop 2 stream')
[streamkfk.stop() for streamkfk in spark.streams.active if streamkfk.name == '2']

print('NASTCH ---- NASTCH')
