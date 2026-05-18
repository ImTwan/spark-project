from pyspark.sql import DataFrame

from src.utils.config import Config


config = Config("/spark/config/spark.conf")

pg_conf = config._get_section_conf("POSTGRESQL")

PG_URL = pg_conf["postgres.jdbc.url"]

PG_USER = pg_conf["postgres.user"]

PG_PASSWORD = pg_conf["postgres.password"]

PG_DRIVER = pg_conf["postgres.jdbc.driver"]


def write_jdbc(df: DataFrame, table: str,mode: str = "append"):
    if df.isEmpty():
        return ( df.write .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", table) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", PG_DRIVER) \
        .mode(mode) \
        .save()
    )


def read_jdbc(spark, table):
    return (
        spark.read \
        .format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", table) \
        .option("user", PG_USER) \
        .option("password", PG_PASSWORD) \
        .option("driver", PG_DRIVER) \
        .load()
    )