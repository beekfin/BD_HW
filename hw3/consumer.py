from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Настройка окружения
os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk'
os.environ['PATH'] = f'{os.environ["JAVA_HOME"]}/bin:{os.environ["PATH"]}'
os.environ['_JAVA_OPTIONS'] = '-Dhadoop.security.authentication=simple -Duser.name=spark'

spark = SparkSession.builder \
    .appName("consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_hw3") \
    .master("local[*]") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField('timestamp', StringType()),
    StructField('stats', MapType(StringType(), LongType()))
])

df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'telegram_words_stream') \
    .option('startingOffsets', 'earliest') \
    .load()

parsed_df = df.select(
    from_json(col('value').cast('string'), schema).alias('data')
).select('data.*')

words_df = parsed_df.select(
    explode(col('stats')).alias('word', 'count')
)

aggregated_df = words_df.groupBy('word').agg(sum('count').alias('total_count'))

top10_df = aggregated_df.orderBy(col('total_count').desc()).limit(10)

query = top10_df.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', 'false') \
    .trigger(processingTime='10 seconds') \
    .start()

print("--- Streaming Query Started ---")
query.awaitTermination()
