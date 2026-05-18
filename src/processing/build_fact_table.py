from pyspark.sql import functions as F


def build_fact_table(enriched_df, dim_product, dim_customer, dim_store, dim_agent, dim_location, dim_date):

    # ========================================================
    # PRODUCT
    # ========================================================

    fact_df = enriched_df.join(

        dim_product.select(
            "sk_product",
            "product_id"
        ),

        on="product_id",

        how="left"
    )


    # ========================================================
    # CUSTOMER
    # ========================================================

    fact_df = fact_df.join(

        dim_customer.select(
            "sk_customer",
            "email",
            "ip"
        ),

        on=[
            "email",
            "ip"
        ],

        how="left"
    )


    # ========================================================
    # STORE
    # ========================================================

    fact_df = fact_df.join(

        dim_store.select(
            "sk_store",
            "store_id"
        ),

        on="store_id",

        how="left"
    )


    # ========================================================
    # AGENT
    # ========================================================

    fact_df = fact_df.join(

        dim_agent.select(
            "sk_agent",
            "browser",
            "os"
        ),

        on=[
            "browser",
            "os"
        ],

        how="left"
    )


    # ========================================================
    # LOCATION
    # ========================================================

    fact_df = fact_df.join(

        dim_location.select(

            "sk_location",

            "country_name_short",

            "country_name_long",

            "city_name",

            "region_name"

        ),

        on=[

            "country_name_short",

            "country_name_long",

            "city_name",

            "region_name"

        ],

        how="left"
    )


    # ========================================================
    # DATE
    # ========================================================

    fact_df = fact_df.withColumn(

        "full_date",

        F.to_timestamp(
            "local_time",
            "yyyy-MM-dd HH:mm:ss"
        )
    )


    fact_df = fact_df.join(

        dim_date.select(
            "sk_date",
            "full_date"
        ),

        on="full_date",

        how="left"
    )


    # ========================================================
    # FINAL FACT
    # ========================================================

    return (

        fact_df.select(

            F.col("id").alias(
                "view_id"
            ),

            "sk_product",

            "sk_store",

            "sk_customer",

            "sk_agent",

            "sk_location",

            "sk_date",

            "api_version",

            "collection",

            "current_url",

            "referrer_url",

            F.to_timestamp(
                "local_time",
                "yyyy-MM-dd HH:mm:ss"
            ).alias("local_time"),

            "time_stamp"
        )
    )