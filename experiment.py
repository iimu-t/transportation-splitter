#!/usr/bin/env python3
from pyspark import SparkConf
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "MyApp") -> SparkSession:
    # SparkConfはデフォルトでSPARK_CONF_DIR（例: /opt/spark/conf/）にあるspark-defaults.confを読み込みます
    conf = SparkConf()
    
    # 追加で必要な設定があれば、ここで設定できます（例：ローカル実行の場合など）
    # conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = create_spark_session("Read S3 Parquet")
    df_final = spark.read.parquet("s3a://overturemaps-data/splitter_results_2_joined/part-00000-393759a9-ee0c-455b-9601-019c97c8d624-c000.zstd.parquet")
    df_final.printSchema()
    df_final.select("geometry_wkt").show(5, truncate=False)
    spark.stop()
