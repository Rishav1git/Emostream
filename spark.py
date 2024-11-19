import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,expr,window,from_json,to_json,explode,array,udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType,ArrayType


schema=StructType([StructField("user_id", StringType(), True),StructField("emoji_type", StringType(), True),StructField("timestamp", TimestampType(), True)])

spark=SparkSession.builder.appName("EmojiAggergator").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3").getOrCreate() #change The version according to your spark

emoji_data_rdd=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","emoji-events").load()

string_emoji_rdd=emoji_data_rdd.selectExpr("CAST(value AS STRING) AS json")

col_emoji_rdd=string_emoji_rdd.select(from_json("json",schema).alias("data")).select("data")

agg_emoji_rdd=col_emoji_rdd.groupBy(window(col("data.timestamp"),"2 seconds"),col("data.emoji_type"))

agg_count_emoji=agg_emoji_rdd.agg(F.count("*").alias("count")).withColumn("agg_count",F.ceil(col("count")/10))

#def print_batch(batch_df, batch_id):
#    print(f"Batch ID: {batch_id}")
#    batch_df.show(truncate=False)


def repeat_emoji(emoji, count):
    return [emoji] * int(count)  

repeat_emoji_udf = udf(repeat_emoji, ArrayType(StringType()))

emoji_repeated = agg_count_emoji.withColumn(
    "emoji_list", 
    repeat_emoji_udf(col("emoji_type"), col("agg_count"))
)

emoji_exploded = emoji_repeated.withColumn("emoji", explode(col("emoji_list")))

output_df = emoji_exploded.select(F.to_json(F.struct(col("emoji"))).alias("value"))

query = (
    output_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "processed-emoji-data")
    .outputMode("update")
    .option("checkpointLocation", "/tmp/spark_checkpoints/processed_emoji_data")
    .start()
)

#query = (agg_count_emoji.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("topic", "processed-emoji-data").outputMode("update").option("checkpointLocation", "/tmp/spark_checkpoints/processed_emoji_data").start())

#query = agg_count_emoji.writeStream.outputMode("update").foreachBatch(print_batch).start()

query.awaitTermination()

