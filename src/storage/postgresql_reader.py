from pyspark.sql import SparkSession


# =====================================================
# READ TABLE
# =====================================================

def read_jdbc_table(spark, table_name, jdbc):

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", table_name)
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", jdbc["driver"])
        .load()
    )


# =====================================================
# WRITE JDBC SAFE
# =====================================================

def write_jdbc(
    df,
    table_name,
    jdbc,
    primary_keys
):

    spark = df.sparkSession

    print(f"CHECK OLD DATA: {table_name}")

    old_df = (
        read_jdbc_table(
            spark,
            table_name,
            jdbc
        )
        .select(*primary_keys)
        .dropDuplicates(primary_keys)
    )

    print(f"REMOVE DUPLICATE INSIDE BATCH: {table_name}")

    df = df.dropDuplicates(primary_keys)

    print(f"REMOVE EXISTING ROWS: {table_name}")

    df = df.join(
        old_df,
        on=primary_keys,
        how="left_anti"
    )

    print(f"WRITE JDBC: {table_name}")

    (
        df.write
        .format("jdbc")
        .option("url", jdbc["url"])
        .option("dbtable", table_name)
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", jdbc["driver"])
        .option("batchsize", 500)
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )

    print(f"✅ WRITE DONE: {table_name}")