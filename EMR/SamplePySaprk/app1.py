import os
from pyspark.sql import SparkSession
import sys


def main():
    spark = SparkSession \
        .builder \
        .appName("csv-to-parquet") \
        .getOrCreate()

    convert_to_parquet(spark)


def convert_to_parquet(spark):
    df_csv = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load("s3://bdmtest0909/indian_food.csv")

    write_parquet(df_csv)

def write_parquet(df_csv):
    df_csv.write \
        .format("parquet") \
        .save("s3://bdmtest0909/output/", mode="overwrite")


if __name__ == "__main__":
    main()
