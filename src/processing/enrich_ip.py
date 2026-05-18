import IP2Location

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from src.utils.config import Config


# =====================================================
# LOAD CONFIG
# =====================================================

config = Config("/spark/config/spark.conf")

ip2location_conf = config._get_section_conf(
    "IP2LOCATION"
)


# =====================================================
# LOAD IP2LOCATION DATABASE
# =====================================================

DB_PATH = ip2location_conf["ip2location.path"]

ip2loc = IP2Location.IP2Location(DB_PATH)


# =====================================================
# SCHEMA
# =====================================================

location_schema = StructType([
    StructField(
        "country_name_short",
        StringType(),
        True
    ),

    StructField(
        "country_name_long",
        StringType(),
        True
    ),

    StructField(
        "city_name",
        StringType(),
        True
    ),

    StructField(
        "region_name",
        StringType(),
        True
    ),
])


# =====================================================
# GET LOCATION
# =====================================================

def get_location(ip):

    try:

        record = ip2loc.get_all(ip)

        return (
            record.country_short,
            record.country_long,
            record.city,
            record.region
        )

    except Exception:

        return (
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown"
        )


# =====================================================
# UDF
# =====================================================

location_udf = F.udf(
    get_location,
    location_schema
)


# =====================================================
# ENRICH IP
# =====================================================

def enrich_ip(df):

    return (
        df.withColumn(
            "location",
            location_udf("ip")
        )

        .withColumn(
            "country_name_short",
            F.col("location.country_name_short")
        )

        .withColumn(
            "country_name_long",
            F.col("location.country_name_long")
        )

        .withColumn(
            "city_name",
            F.col("location.city_name")
        )

        .withColumn(
            "region_name",
            F.col("location.region_name")
        )

        .drop("location")
    )