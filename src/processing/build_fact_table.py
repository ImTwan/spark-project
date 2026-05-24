import pyspark.sql.functions as F


def build_fact_table(
    df,
    dim_customer,
    dim_agent,
    dim_location,
    dim_date,
    dim_store,
    dim_product
):

    # =====================================================
    # CLEAN
    # =====================================================

    df = (
        df
        .filter(F.col("id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("store_id").isNotNull())
    )

    # =====================================================
    # FIX LOCAL TIME
    # =====================================================

    df = df.withColumn(
        "local_time",
        F.to_timestamp("local_time")
    )

    # =====================================================
    # JOIN CUSTOMER
    # =====================================================

    customer = dim_customer.select(
        "email",
        "ip",
        "sk_customer"
    )

    df = df.join(
        customer,
        ["email", "ip"],
        "left"
    )

    # =====================================================
    # JOIN AGENT
    # =====================================================

    agent = dim_agent.select(
        "browser",
        "os",
        "sk_agent"
    )

    df = df.join(
        agent,
        ["browser", "os"],
        "left"
    )

    # =====================================================
    # JOIN LOCATION
    # =====================================================

    location = dim_location.select(
        "country_name_long",
        "city_name",
        "region_name",
        "sk_location"
    )

    df = df.join(
        location,
        ["country_name_long", "city_name", "region_name"],
        "left"
    )

    # =====================================================
    # JOIN DATE
    # =====================================================

    date_dim = dim_date.select(
        "full_date",
        "hour",
        "minute",
        "sk_date"
    )

    df = (
        df
        .withColumn(
            "full_date",
            F.to_date("local_time")
        )
        .withColumn(
            "hour",
            F.hour("local_time")
        )
        .withColumn(
            "minute",
            F.minute("local_time")
        )
    )

    df = df.join(
        date_dim,
        ["full_date", "hour", "minute"],
        "left"
    )

    # =====================================================
    # FILL UNKNOWN KEYS
    # =====================================================

    df = (
        df
        .fillna(
            {
                "sk_customer": 0,
                "sk_agent": 0,
                "sk_location": 0,
                "sk_date": 0
            }
        )
    )

    # =====================================================
    # FINAL FACT
    # =====================================================

    return (
        df.select(
            "id",
            "product_id",
            "store_id",
            "sk_customer",
            "sk_agent",
            "sk_location",
            "sk_date",
            "api_version",
            "collection",
            "current_url",
            "referrer_url",
            "local_time",
            "time_stamp"
        )
        .dropDuplicates(["id"])
    )