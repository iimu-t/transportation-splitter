#!/usr/bin/env python3
from pyspark.sql import SparkSession

# SparkSession の作成
spark = SparkSession.builder \
    .appName("Read Connector Parquet Columns") \
    .getOrCreate()

# Parquet ファイルの読み込み
#df = spark.read.parquet("./test/data/connector.parquet")
df = spark.read.parquet("./test/data/segment.parquet")

# カラム名の取得と出力
print("connector.parquet のカラム名:")
for col in df.columns:
    print(col)
