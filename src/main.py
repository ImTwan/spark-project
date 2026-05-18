from pyspark.sql.functions import col

from pyspark.sql import SparkSession
import traceback

from src.utils.config import Config
from src.utils.logger import Log4j

from src.ingestion.kafka_streaming import read_kafka_stream

from src.processing.parse_json import parse_json
from src.processing.enrich_ip import enrich_ip
from src.processing.enrich_agent import enrich_agent

from src.processing.build_dim_table import (
    build_dim_product,
    build_dim_customer,
    build_dim_store,
    build_dim_agent,
    build_dim_location,
    build_dim_date
)

from src.processing.build_fact_table import build_fact_table
from src.storage.postgresql_reader import write_jdbc


config = Config("/spark/config/spark.conf")


def process_batch(batch_df, batch_id):
    try:

        if batch_df.isEmpty():

            print(f"Batch {batch_id} is empty")

            return

        print(f"Processing batch {batch_id}")

        # =====================================================
        # ENRICH DATA
        # =====================================================

        enriched_df = enrich_ip(batch_df)

        enriched_df = enrich_agent(enriched_df)

        # =====================================================
        # BUILD DIM TABLES
        # =====================================================

        dim_product = build_dim_product(enriched_df)

        dim_customer = build_dim_customer(enriched_df)

        dim_store = build_dim_store(enriched_df)

        dim_agent = build_dim_agent(enriched_df)

        dim_location = build_dim_location(enriched_df)

        dim_date = build_dim_date(enriched_df)

        # =====================================================
        # WRITE DIM TABLES
        # =====================================================

        write_jdbc(dim_product, "dim_product")

        write_jdbc(dim_customer, "dim_customer")

        write_jdbc(dim_store, "dim_store")

        write_jdbc(dim_agent, "dim_agent")

        write_jdbc(dim_location, "dim_location")

        write_jdbc(dim_date, "dim_date")

        # =====================================================
        # BUILD FACT TABLE
        # =====================================================

        fact_df = build_fact_table(
            enriched_df,
            dim_product,
            dim_customer,
            dim_store,
            dim_agent,
            dim_location,
            dim_date
        )

        # =====================================================
        # WRITE FACT TABLE
        # =====================================================

        write_jdbc(
            fact_df,
            "fact_product_view"
        )

        print(f"Batch {batch_id} completed")

    except Exception:

        print("================================")
        print(f"BATCH ERROR: {batch_id}")
        print("================================")

        traceback.print_exc()


def main():

    try:

        # =====================================================
        # CREATE SPARK SESSION
        # =====================================================

        spark = (
            SparkSession.builder
            .config(conf=config.spark_conf)
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        logger = Log4j(spark)

        logger.info("Spark session created")

        # =====================================================
        # READ KAFKA STREAM
        # =====================================================

        kafka_df = read_kafka_stream(spark)

        logger.info("Kafka stream connected")

        # =====================================================
        # CONVERT VALUE TO STRING
        # =====================================================

        json_df = kafka_df.select(
            col("value").cast("string").alias("json_data")
        )

        # =====================================================
        # PRINT STREAM
        # =====================================================

        query = (
            json_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 20)
            .start()
        )

        logger.info("Streaming started")

        query.awaitTermination()

    except Exception:

        print("================================")
        print("STREAMING ERROR")
        print("================================")

        traceback.print_exc()


if __name__ == "__main__":
    main()