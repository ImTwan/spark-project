import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from user_agents import parse

def extract_browser(ua):
    try:
        return parse(ua).browser.family
    except:
        return "UNKNOWN"

def extract_os(ua):
    try:
        return parse(ua).os.family
    except:
        return "UNKNOWN"

browser_udf = F.udf(extract_browser, StringType())
os_udf = F.udf(extract_os, StringType())

def enrich_agent(df):
    return (
        df
        .withColumn(
            "browser",
            browser_udf(F.col("user_agent"))
        )
        .withColumn(
            "os",
            os_udf(F.col("user_agent"))
        )
    )