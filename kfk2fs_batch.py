from time import sleep
import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, decode

def compute_batch(df: DataFrame):
    # select всех полей, преобразуем value т.к. он закодирован
    df_decode = df \
        .select(col('value').cast('string'), col('topic'), col('partition'), col('offset'), col('timestamp'))
    (df_decode
     .filter("topic = 'ark-topic-1'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-1'))
    (df_decode
     .filter("topic = 'ark-topic-2'")
     .write
     .format('json')
     .mode('append')
     .option('encoding', 'UTF-8')
     .save('/home/aramis2008/sparkstreamingFromKafka/outputStreaming2/topic-2'))
    (df_decode
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

topic_name_1 = 'ark-topic-1'
topic_name_2 = 'ark-topic-2'
topic_name_3 = 'ark-topic-3'

# Если запросить оффсет меньше существующего, то при failOnDataLoss=False ошибка хоть и будет в логе, но приложение не прервется, данные которые выше все равно загрузятся
# Если запросить оффсет больше существующего, то при failOnDataLoss=False будет ошибка которая прервет все
# startingOffsets = f"""{{"{topic_name_1}":{{"0":1000}},"{topic_name_2}":{{"0":1000}},"{topic_name_3}":{{"0":1700}} }}"""
startingOffsets = "earliest"

df = (spark
          .read
          .format('kafka')
          .option('kafka.bootstrap.servers', 'localhost:9092')
          .option('subscribe', f'{topic_name_1},{topic_name_2},{topic_name_3}')
          .option("startingOffsets", startingOffsets)
          .option("failOnDataLoss", False)
          .option('checkpointLocation', '/home/aramis2008/sparkstreamingFromKafka/checkpoint')
          .load())


print(datetime.datetime.now().time(), '1 batch start')
compute_batch(df)
print(datetime.datetime.now().time(), '1 batch finish')