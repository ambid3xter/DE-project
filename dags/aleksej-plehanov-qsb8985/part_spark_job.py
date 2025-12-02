import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PART_PATH = "s3a://de-raw/part"
PARTSUPP_PATH = "s3a://de-raw/partsupp"
SUPPLIER_PATH = "s3a://de-raw/supplier"
NATION_PATH = "s3a://de-raw/nation"
TARGET_PATH = "s3a://de-project/aleksej-plehanov-qsb8985/parts_report"


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

    df_part = spark.read.parquet(PART_PATH)
    df_partsupp = spark.read.parquet(PARTSUPP_PATH)
    df_supplier = spark.read.parquet(SUPPLIER_PATH)
    df_nation = spark.read.parquet(NATION_PATH)

    joined_df = df_part \
        .join(df_partsupp,
              df_part.P_PARTKEY == df_partsupp.PS_PARTKEY) \
        .join(df_supplier,
              df_partsupp.PS_SUPPKEY == df_supplier.S_SUPPKEY) \
        .join(df_nation,
              df_supplier.S_NATIONKEY == df_nation.N_NATIONKEY)
    
    result_df = joined_df.groupBy(
        "N_NAME", "P_TYPE", "P_CONTAINER"
    ).agg(
        F.count("P_PARTKEY").alias("parts_count"),
        F.avg("P_RETAILPRICE").alias("avg_retailprice"),
        F.sum("P_SIZE").alias("size"),
        F.expr("percentile_approx(P_RETAILPRICE, 0.5)").alias("mean_retailprice"),
        F.min("P_RETAILPRICE").alias("min_retailprice"),
        F.max("P_RETAILPRICE").alias("max_retailprice"),
        F.avg("PS_SUPPLYCOST").alias("avg_supplycost"),
        F.expr("percentile_approx(PS_SUPPLYCOST, 0.5)").alias("mean_supplycost"),
        F.min("PS_SUPPLYCOST").alias("min_supplycost"),
        F.max("PS_SUPPLYCOST").alias("max_supplycost")
    ).orderBy("N_NAME", "P_TYPE", "P_CONTAINER")

    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
