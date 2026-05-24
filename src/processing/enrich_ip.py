import IP2Location
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("country_name_long", StringType(), True),
    StructField("region_name", StringType(), True),
    StructField("city_name", StringType(), True)
])

_ip_db = None

def _get_db():
    global _ip_db
    if _ip_db is None:
        _ip_db = IP2Location.IP2Location("IP-COUNTRY-REGION-CITY.BIN")
    return _ip_db


@F.udf(schema)
def get_ip_info(ip):
    try:
        db = _get_db()
        result = db.get_all(ip)

        return (
            result.country_long or "UNKNOWN",
            result.region or "UNKNOWN",
            result.city or "UNKNOWN"
        )
    except Exception:
        return ("UNKNOWN", "UNKNOWN", "UNKNOWN")


def enrich_ip(df):
    return (
        df.withColumn("ip_info", get_ip_info("ip"))
          .withColumn("country_name_long", F.col("ip_info.country_name_long"))
          .withColumn("region_name", F.col("ip_info.region_name"))
          .withColumn("city_name", F.col("ip_info.city_name"))
          .drop("ip_info")
    )