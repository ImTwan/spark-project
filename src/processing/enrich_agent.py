from user_agents import parse

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


agent_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
])


def parse_agent(user_agent):

    try:

        ua = parse(user_agent)

        return (
            ua.browser.family,
            ua.os.family
        )

    except Exception:

        return (
            "Unknown",
            "Unknown"
        )


agent_udf = F.udf(
    parse_agent,
    agent_schema
)


def enrich_agent(df):

    return (
        df.withColumn(
            "agent",
            agent_udf("user_agent")
        )
        .withColumn(
            "browser",
            F.col("agent.browser")
        )
        .withColumn(
            "os",
            F.col("agent.os")
        )
        .drop("agent")
    )