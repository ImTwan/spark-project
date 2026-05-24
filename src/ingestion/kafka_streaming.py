from src.utils.config import Config

config = Config("/spark/config/spark.conf")


def read_kafka_stream(spark):

    kafka_conf = config.kafka_conf

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_conf["kafka_bootstrap_servers"])
        .option("subscribe", kafka_conf["kafka_topic"])
        .option("kafka.security.protocol", kafka_conf["kafka_security_protocol"])
        .option("kafka.sasl.mechanism", kafka_conf["kafka_sasl_mechanism"])
        .option("kafka.sasl.jaas.config", kafka_conf["kafka_sasl_jaas_config"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)

        # tránh crash khi topic đổi partition
        .option("failOnDataLoss", "false")

        .load()
    )