from pyspark.sql import SparkSession

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
          .option('checkpointLocation', './.local/checkpoint')
          .load())

(source
     .writeStream
     .format("console")
     .start()
     .awaitTermination())