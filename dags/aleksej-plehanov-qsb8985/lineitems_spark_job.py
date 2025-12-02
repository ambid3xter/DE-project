import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LINES_PATH = "s3a://de-raw/lineitem"
TARGET_PATH = "s3a://de-project/aleksej-plehanov-qsb8985/lineitems_report"


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

    lineitems_df = spark.read.parquet(LINES_PATH)

    df = (lineitems_df
          .withColumn("L_SHIPDATE", F.to_date("L_SHIPDATE"))
          .withColumn("L_RECEIPTDATE", F.to_date("L_RECEIPTDATE")))

    result_df = df.groupBy("L_ORDERKEY").agg(
        F.count("*").alias("count"),
        F.sum("L_EXTENDEDPRICE").alias("sum_extendprice"),
        F.expr("percentile_approx(L_DISCOUNT, 0.5)").alias("mean_discount"),
        F.avg("L_TAX").alias("mean_tax"),
        F.avg(F.datediff(F.col("L_RECEIPTDATE"), F.col("L_SHIPDATE"))).alias("delivery_days"),
        F.sum(F.when(F.col("L_RETURNFLAG") == "A", 1).otherwise(0)).alias("A_return_flags"),
        F.sum(F.when(F.col("L_RETURNFLAG") == "R", 1).otherwise(0)).alias("R_return_flags"),
        F.sum(F.when(F.col("L_RETURNFLAG") == "N", 1).otherwise(0)).alias("N_return_flags"),
    ).orderBy("L_ORDERKEY")

    result_df.write.mode("overwrite").parquet(TARGET_PATH)

    spark.stop()


if __name__ == "__main__":
    main()