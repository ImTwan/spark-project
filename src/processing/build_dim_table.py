import pyspark.sql.functions as F


# =====================================================
# PRODUCT DIM
# =====================================================
def build_dim_product(df):

    return (
        df
        .filter(F.col("product_id").isNotNull())
        .filter(F.trim(F.col("product_id")) != "")

        .withColumn(
            "option_text",
            F.when(
                F.col("option").isNotNull(),
                F.concat_ws(
                    ",",
                    F.expr("transform(option, x -> cast(x.option_label as string))")
                )
            ).otherwise("UNKNOWN")
        )

        .select(
            F.col("product_id").cast("string"),
            F.col("option_text").cast("string").alias("option")
        )

        .dropDuplicates(["product_id"])
    )


# =====================================================
# CUSTOMER DIM
# =====================================================
def build_dim_customer(df):

    return (
        df
        .filter(F.col("email").isNotNull())
        .filter(F.col("ip").isNotNull())

        .select("email", "ip")

        .withColumn(
            "sk_customer",
            F.abs(F.hash("email", "ip")) % 2147483647
        )

        .dropDuplicates(["sk_customer"])
    )


# =====================================================
# STORE DIM
# =====================================================
def build_dim_store(df):

    return (
        df
        .filter(F.col("store_id").isNotNull())

        .select("store_id")

        .withColumn(
            "store_name",
            F.concat(F.lit("store "), F.col("store_id"))
        )

        .dropDuplicates(["store_id"])
    )


# =====================================================
# AGENT DIM
# =====================================================
def build_dim_agent(df):

    return (
        df
        .filter(F.col("browser").isNotNull())
        .filter(F.col("os").isNotNull())

        .select("browser", "os")

        .withColumn(
            "sk_agent",
            F.abs(F.hash("browser", "os")) % 2147483647
        )

        .dropDuplicates(["sk_agent"])
    )


# =====================================================
# LOCATION DIM
# =====================================================
def build_dim_location(df):

    return (
        df
        .filter(F.col("country_name_long").isNotNull())
        .filter(F.col("city_name").isNotNull())
        .filter(F.col("region_name").isNotNull())

        .select(
            "country_name_long",
            "city_name",
            "region_name"
        )

        .withColumn(
            "sk_location",
            F.abs(
                F.hash(
                    "country_name_long",
                    "city_name",
                    "region_name"
                )
            ) % 2147483647
        )

        .dropDuplicates(["sk_location"])
    )


# =====================================================
# DATE DIM
# =====================================================
def build_dim_date(df):

    df = df.withColumn(
        "local_time",
        F.to_timestamp("local_time")
    )

    return (
        df
        .filter(F.col("local_time").isNotNull())

        .withColumn(
            "full_date",
            F.to_date("local_time")
        )

        .withColumn(
            "sk_date",
            (
                F.abs(
                    F.hash(
                        F.date_format(
                            "local_time",
                            "yyyy-MM-dd HH:mm"
                        )
                    )
                ) % 2147483647
            ).cast("int")
        )

        .select(
            "sk_date",
            "full_date",

            F.dayofweek("local_time").alias("day_of_week"),
            F.dayofmonth("local_time").alias("day_of_month"),
            F.dayofyear("local_time").alias("day_of_year"),

            F.date_format(
                "local_time",
                "yyyy-MM"
            ).alias("year_month"),

            F.month("local_time").alias("month"),

            F.weekofyear("local_time").alias("week_of_year"),

            F.quarter("local_time").alias("quarter_number"),

            F.year("local_time").alias("year"),

            F.expr("""
                CASE
                    WHEN dayofweek(local_time) IN (1,7)
                    THEN true
                    ELSE false
                END
            """).alias("is_weekend"),
            F.hour("local_time").alias("hour"),
            F.minute("local_time").alias("minute")
        )
        .dropDuplicates(["sk_date"])
    )