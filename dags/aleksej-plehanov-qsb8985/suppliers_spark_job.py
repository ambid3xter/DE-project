import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SUPPLIER_PATH = "s3a://de-raw/supplier"
REGION_PATH = "s3a://de-raw/region"
NATION_PATH = "s3a://de-raw/nation"
TARGET_PATH = "s3a://de-project/aleksej-plehanov-qsb8985/suppliers_report"


def _spark_session():
    return (SparkSession.builder
            .appName("SparkJob-parts-" + uuid.uuid4().hex)
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
            .config('spark.hadoop.fs.s3a.endpoint', "https://hb.bizmrg.com")
            .config('spark.hadoop.fs.s3a.region', "ru-msk")
            .config("spark.hadoop.fs.s3a.access.key",os.environ["S3_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key",os.environ["S3_SECRET_KEY"])
            .getOrCreate())


def main():
    spark = _spark_session()

    df_supplier = spark.read.parquet(SUPPLIER_PATH)
    df_region = spark.read.parquet(REGION_PATH)
    df_nation = spark.read.parquet(NATION_PATH)

    joined_df = df_supplier \
        .join(df_nation,
              df_supplier.S_NATIONKEY == df_nation.N_NATIONKEY) \
        .join(df_region,
              df_nation.N_REGIONKEY == df_region.R_REGIONKEY)
    
    result_df = joined_df.groupBy(
        "R_NAME", "N_NAME"
    ).agg(
        F.countDistinct("S_SUPPKEY").alias("unique_supplers_count"),
        F.avg("S_ACCTBAL").alias("avg_acctbal"),
        F.expr("percentile_approx(S_ACCTBAL, 0.5)").alias("mean_acctbal"),
        F.min("S_ACCTBAL").alias("min_acctbal"),
        F.max("S_ACCTBAL").alias("max_acctbal")
    )

    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()