import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)

    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    # df.printSchema()
    schema = StructType([
        StructField("id", StringType()),
        StructField("time_stamp", LongType()),
        StructField("ip", StringType()),
        StructField("user_agent", StringType()),
        StructField("resolution", StringType()),
        StructField("device_id", StringType()),
        StructField("api_version", StringType()),
        StructField("store_id", StringType()),
        StructField("local_time", StringType()),
        StructField("show_recommendation", StringType()),
        StructField("current_url", StringType()),
        StructField("referrer_url", StringType()),
        StructField("email_address", StringType()),
        StructField("collection", StringType()),
        StructField("product_id", StringType()),
        StructField("option", ArrayType(MapType(StringType(), StringType())))
    ])

    structured_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")



    # query = df.select(col("value").cast(StringType()).alias("value")) \
    #     .writeStream \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .trigger(processingTime="30 seconds") \
    #     .start()

    # query.awaitTermination()

    query = structured_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

    query.awaitTermination()
