from pyspark.sql import functions as F


# ============================================================
# DIM PRODUCT
# ============================================================

def build_dim_product(df):

    return (

        df.select(
            "product_id",

            F.when(
                F.size("option") > 0,
                F.col("option")[0]["option_label"]
            ).otherwise(None).alias("option")

        )

        .filter(
            F.col("product_id").isNotNull()
        )

        .dropDuplicates([
            "product_id"
        ])
    )


# ============================================================
# DIM CUSTOMER
# ============================================================

def build_dim_customer(df):

    return (

        df.select(
            "email",
            "ip"
        )

        .filter(
            F.col("ip").isNotNull()
        )

        .dropDuplicates([
            "email",
            "ip"
        ])
    )


# ============================================================
# DIM STORE
# ============================================================

def build_dim_store(df):

    return (

        df.select(
            "store_id"
        )

        .withColumn(
            "store_name",
            F.concat(
                F.lit("Store "),
                F.col("store_id")
            )
        )

        .dropDuplicates([
            "store_id"
        ])
    )


# ============================================================
# DIM AGENT
# ============================================================

def build_dim_agent(df):

    return (

        df.select(
            "browser",
            "os"
        )

        .dropDuplicates([
            "browser",
            "os"
        ])
    )


# ============================================================
# DIM LOCATION
# ============================================================

def build_dim_location(df):

    return (
        df.select(
            "country_name_short",
            "country_name_long",
            "city_name",
            "region_name"
        )
        .dropDuplicates([
            "country_name_short",
            "country_name_long",
            "city_name",
            "region_name"
        ])
    )

# ============================================================
# DIM DATE
# ============================================================

def build_dim_date(df):

    ts_col = F.to_timestamp(
        "local_time",
        "yyyy-MM-dd HH:mm:ss"
    )

    return (

        df.withColumn(
            "full_date",
            ts_col
        )

        .select(

            "full_date",

            F.dayofweek("full_date").alias(
                "day_of_week"
            ),

            F.dayofmonth("full_date").alias(
                "day_of_month"
            ),

            F.dayofyear("full_date").alias(
                "day_of_year"
            ),

            F.date_format(
                "full_date",
                "yyyy-MM"
            ).alias(
                "year_month"
            ),

            F.month("full_date").alias(
                "month"
            ),

            F.weekofyear("full_date").alias(
                "week_of_year"
            ),

            F.quarter("full_date").alias(
                "quarter_number"
            ),

            F.year("full_date").alias(
                "year"
            ),

            F.year("full_date").alias(
                "year_number"
            ),

            F.when(
                F.dayofweek("full_date").isin([1, 7]),
                True
            ).otherwise(False).alias(
                "is_weekend"
            ),

            F.hour("full_date").alias(
                "hour"
            ),

            F.minute("full_date").alias(
                "minute"
            )
        )

        .dropDuplicates([
            "full_date"
        ])
    )