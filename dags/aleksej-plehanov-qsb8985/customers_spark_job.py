import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CUSTOMER_PATH = "s3a://de-raw/customer"
NATION_PATH = "s3a://de-raw/nation"
REGION_PATH = "s3a://de-raw/region"
TARGET_PATH = "s3a://de-project/aleksej-plehanov-qsb8985/customers_report"


def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob-orders-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config("spark.hadoop.fs.s3a.access.key",os.environ["S3_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key",os.environ["S3_SECRET_KEY"])
            .getOrCreate())


def main():
    spark = _spark_session()

    df_customer = spark.read.parquet(CUSTOMER_PATH)
    df_nation = spark.read.parquet(NATION_PATH)
    df_region = spark.read.parquet(REGION_PATH)

    joined_df = df_customer \
    .join(
        df_nation,
        df_customer.C_NATIONKEY == df_nation.N_NATIONKEY        
    ) \
    .join(
        df_region,
        df_nation.N_REGIONKEY == df_region.R_REGIONKEY
    )

    result_df = joined_df.groupBy(
        "R_NAME", "N_NAME", "C_MKTSEGMENT"
    ).agg(
        F.countDistinct("C_CUSTKEY").alias("unique_customers_count"),
        F.avg("C_ACCTBAL").alias("avg_acctbal"),
        F.expr("percentile_approx(C_ACCTBAL, 0.5)").alias("mean_acctbal"),
        F.min("C_ACCTBAL").alias("min_acctbal"),
        F.max("C_ACCTBAL").alias("max_acctbal")
    ).orderBy("R_NAME", "N_NAME", "C_MKTSEGMENT")

    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()