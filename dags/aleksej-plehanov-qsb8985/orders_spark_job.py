import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ORDERS_PATH = "s3a://de-raw/orders"
CUSTOMER_PATH = "s3a://de-raw/customer"
NATION_PATH = "s3a://de-raw/nation"
TARGET_PATH = "s3a://de-project/aleksej-plehanov-qsb8985/orders_report"


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

    orders_df = spark.read.parquet(ORDERS_PATH)
    customer_df = spark.read.parquet(CUSTOMER_PATH)
    nation_df = spark.read.parquet(NATION_PATH)

    joined_df = orders_df.join(
        customer_df,
        orders_df.O_CUSTKEY == customer_df.C_CUSTKEY,
        "left"
    ).join(
        nation_df,
        customer_df.C_NATIONKEY == nation_df.N_NATIONKEY,
        "left"
    )

    result_df = joined_df.withColumn(
        "O_MONTH",
        F.date_format("O_ORDERDATE", "yyyy-MM")
    ).groupBy(
        "O_MONTH", "N_NAME", "O_ORDERPRIORITY"
    ).agg(
        F.count("*").alias("orders_count"),
        F.avg("O_TOTALPRICE").alias("avg_order_price"),
        F.sum("O_TOTALPRICE").alias("sum_order_price"),
        F.min("O_TOTALPRICE").alias("min_order_price"),
        F.max("O_TOTALPRICE").alias("max_order_price"),
        F.sum(F.when(F.col("O_ORDERSTATUS") == "F", 1).otherwise(0)).alias("f_order_status"),
        F.sum(F.when(F.col("O_ORDERSTATUS") == "O", 1).otherwise(0)).alias("o_order_status"),
        F.sum(F.when(F.col("O_ORDERSTATUS") == "P", 1).otherwise(0)).alias("p_order_status")
    ).orderBy("N_NAME", "O_ORDERPRIORITY")

    result_df.write.mode("overwrite").parquet(TARGET_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
