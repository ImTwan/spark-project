from pyspark.sql import SparkSession
from src.utils.config import Config

config = Config("/spark/config/spark.conf")


def read_kafka_stream(spark: SparkSession):

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            config.kafka_conf["kafka_bootstrap_servers"]
        )
        .option(
            "subscribe",
            config.kafka_conf["kafka_topic"]
        )
        .option(
            "kafka.security.protocol",
            config.kafka_conf["kafka_security_protocol"]
        )
        .option(
            "kafka.sasl.mechanism",
            config.kafka_conf["kafka_sasl_mechanism"]
        )
        .option(
            "kafka.sasl.jaas.config",
            config.kafka_conf["kafka_sasl_jaas_config"]
        )
        .option("startingOffsets", "latest")
        .load()
    )

    return kafka_df