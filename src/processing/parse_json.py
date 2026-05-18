from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType


# Schema cho option
option_schema = StructType([
    StructField("option_id", StringType(), True),
    StructField("option_label", StringType(), True),
])

# Schema chính
log_schema = StructType([
    StructField("id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("local_time", StringType(), True),
    StructField("option", ArrayType(option_schema), True),
    StructField("product_id", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("user_agent", StringType(), True),
])


def parse_json(df):

    df_parsed = (
        df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), log_schema).alias("data")) \
        .select("data.*")
    )

    return df_parsed