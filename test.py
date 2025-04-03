from pyspark.sql import SparkSession

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark = (SparkSession
         .builder
         .appName('SparkByExamples')
         .getOrCreate())
df = spark.sparkContext.parallelize(data).toDF(columns)

df.show()