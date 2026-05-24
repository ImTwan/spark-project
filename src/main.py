from pyspark.sql import SparkSession
import traceback

from src.utils.config import Config
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


# =====================================================
# CONFIG
# =====================================================

config = Config("/spark/config/spark.conf")

postgres_conf = config._get_section_conf("POSTGRESQL")

jdbc = {
    "url": postgres_conf["postgres.jdbc.url"],
    "user": postgres_conf["postgres.user"],
    "password": postgres_conf["postgres.password"],
    "driver": postgres_conf["postgres.jdbc.driver"]
}


# =====================================================
# PROCESS BATCH
# =====================================================

def process_batch(batch_df, batch_id):
    try:

        print("\n" + "=" * 80)
        print(f"🚀 START BATCH {batch_id}")
        print("=" * 80)

        # =====================================================
        # PARSE JSON
        # =====================================================

        print("1. PARSE JSON")

        df = parse_json(batch_df)

        print("PARSED SCHEMA")

        df.printSchema()

        # =====================================================
        # ENRICH
        # =====================================================

        print("2. ENRICH IP")

        df = enrich_ip(df)

        print("3. ENRICH AGENT")

        df = enrich_agent(df)

        # =====================================================
        # BUILD DIM
        # =====================================================

        print("4. BUILD DIM TABLES")

        dim_product = build_dim_product(df)
        dim_customer = build_dim_customer(df)
        dim_store = build_dim_store(df)
        dim_agent = build_dim_agent(df)
        dim_location = build_dim_location(df)
        dim_date = build_dim_date(df)

        # =====================================================
        # WRITE DIM
        # =====================================================

        print("5. WRITE DIM TABLES")

        write_jdbc(
            dim_product,
            "dim_product",
            jdbc,
            ["product_id"]
        )

        write_jdbc(
            dim_customer,
            "dim_customer",
            jdbc,
            ["sk_customer"]
        )

        write_jdbc(
            dim_store,
            "dim_store",
            jdbc,
            ["store_id"]
        )

        write_jdbc(
            dim_agent,
            "dim_agent",
            jdbc,
            ["sk_agent"]
        )

        write_jdbc(
            dim_location,
            "dim_location",
            jdbc,
            ["sk_location"]
        )

        write_jdbc(
            dim_date,
            "dim_date",
            jdbc,
            ["sk_date"]
        )

        print("✅ ALL DIM DONE")

        # =====================================================
        # BUILD FACT
        # =====================================================

        print("6. BUILD FACT")

        fact_df = build_fact_table(
            df,
            dim_customer,
            dim_agent,
            dim_location,
            dim_date,
            dim_store,
            dim_product
        )

        # =====================================================
        # WRITE FACT
        # =====================================================

        print("7. WRITE FACT")

        write_jdbc(
            fact_df,
            "fact_product_view",
            jdbc,
            ["id"]
        )

        print("✅ FACT DONE")

        print(f"🎉 FINISH BATCH {batch_id}")

    except Exception:

        print("❌ ERROR IN BATCH")

        traceback.print_exc()

# =====================================================
# MAIN
# =====================================================
def main():

    spark = (
        SparkSession.builder
        .appName("streaming")

        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "1")

        .config("spark.executor.memory", "3g")
        .config("spark.driver.memory", "3g")

        .config("spark.sql.shuffle.partitions", "1")

        .config("spark.sql.adaptive.enabled", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("🚀 START STREAMING JOB")

    kafka_df = read_kafka_stream(spark)

    query = (
        kafka_df.writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            config._get_section_conf("STREAMING")[
                "checkpoint.location"
            ]
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()